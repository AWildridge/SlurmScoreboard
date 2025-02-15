#!/usr/bin/env python3
import os
import csv
import time
import fcntl
import argparse

DEFAULT_INTERVAL = 86400  # seconds (24 hours)

def get_users(project_folder):
    """List all users by scanning subdirectories under PROJECT_FOLDER/data."""
    data_dir = os.path.join(project_folder, "data")
    if not os.path.exists(data_dir):
        return []
    return [entry for entry in os.listdir(data_dir) 
            if os.path.isdir(os.path.join(data_dir, entry))]

def get_known_clusters(project_folder):
    """Return a set of known cluster names from known_clusters.txt."""
    known_file = os.path.join(project_folder, "data", "known_clusters.txt")
    if not os.path.exists(known_file):
        return set()
    with open(known_file, "r") as f:
        fcntl.flock(f, fcntl.LOCK_SH)
        clusters = {line.strip() for line in f if line.strip()}
        fcntl.flock(f, fcntl.LOCK_UN)
    return clusters

def parse_time_to_hours(time_str):
    """
    Convert a time string (format HH:MM:SS or D-HH:MM:SS) to hours.
    """
    if '-' in time_str:
        days_str, hms = time_str.split('-')
        days = int(days_str)
    else:
        days = 0
        hms = time_str
    parts = hms.split(':')
    if len(parts) != 3:
        return 0.0
    hours, minutes, seconds = map(int, parts)
    return days * 24 + hours + minutes / 60.0 + seconds / 3600.0

def aggregate_stats_for_user(project_folder, user, clusters):
    """
    For a given user, read each processed_jobs CSV (one per cluster) and aggregate:
      - Clock hours (elapsed time),
      - Memory hours (elapsed time * memory usage in GB),
      - GPU hours (elapsed time if the job used GPUs).
    """
    total_clock = 0.0
    total_mem = 0.0
    total_gpu = 0.0

    for cluster in clusters:
        processed_csv = os.path.join(project_folder, "data", user, "processed_jobs", f"{cluster}.csv")
        if not os.path.exists(processed_csv):
            continue
        with open(processed_csv, "r") as f:
            fcntl.flock(f, fcntl.LOCK_SH)
            reader = csv.DictReader(f)
            for row in reader:
                elapsed = row.get("Elapsed", "0:00:00")
                hours = parse_time_to_hours(elapsed)
                total_clock += hours

                try:
                    maxrss = row.get("MaxRSS", "0")
                    # Assume MaxRSS is numeric in MB
                    mem_usage_mb = float(maxrss)
                except ValueError:
                    mem_usage_mb = 0.0
                total_mem += (mem_usage_mb / 1000.0) * hours  # convert MB to GB-hours

                if row.get("GPU_Flag", "False").lower() in ["true", "yes", "1"]:
                    total_gpu += hours
            fcntl.flock(f, fcntl.LOCK_UN)
    return total_clock, total_mem, total_gpu

def build_scoreboard(project_folder, scoreboard_file):
    """
    Aggregates statistics for each user and writes a scoreboard CSV file.
    The CSV will have columns: User, ClockHours, MemoryHours, GPUHours.
    """
    users = get_users(project_folder)
    clusters = get_known_clusters(project_folder)
    aggregated = []

    for user in users:
        clock, mem, gpu = aggregate_stats_for_user(project_folder, user, clusters)
        aggregated.append({
            "User": user,
            "ClockHours": f"{clock:.2f}",
            "MemoryHours": f"{mem:.2f}",
            "GPUHours": f"{gpu:.2f}"
        })

    # Sort by ClockHours descending (change as needed)
    aggregated.sort(key=lambda x: float(x["ClockHours"]), reverse=True)

    os.makedirs(os.path.dirname(scoreboard_file), exist_ok=True)
    with open(scoreboard_file, "w", newline="") as csvfile:
        fcntl.flock(csvfile, fcntl.LOCK_EX)
        fieldnames = ["User", "ClockHours", "MemoryHours", "GPUHours"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in aggregated:
            writer.writerow(row)
        fcntl.flock(csvfile, fcntl.LOCK_UN)
    print("Scoreboard updated.")

def main():
    parser = argparse.ArgumentParser(description="Scoreboard Builder Daemon")
    parser.add_argument("--project-folder", default="/depot/cms/top/awildrid/SlurmScoreboard",
                        help="Path to the project folder")
    parser.add_argument("--scoreboard-file", default="/depot/cms/top/awildrid/SlurmScoreboard/scoreboard.csv",
                        help="Output file for the scoreboard")
    parser.add_argument("--interval", type=int, default=DEFAULT_INTERVAL,
                        help="Interval in seconds between scoreboard updates (default: 86400)")
    args = parser.parse_args()

    project_folder = args.project_folder.rstrip("/")
    scoreboard_file = args.scoreboard_file

    while True:
        print("Building scoreboard...")
        build_scoreboard(project_folder, scoreboard_file)
        print(f"Sleeping for {args.interval} seconds...")
        time.sleep(args.interval)

if __name__ == "__main__":
    main()

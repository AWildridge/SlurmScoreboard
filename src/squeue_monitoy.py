#!/usr/bin/env python3
import os
import subprocess
import time
import fcntl
import argparse

POLL_INTERVAL = 30  # seconds

def get_known_clusters(project_folder):
    """Return a set of known cluster names stored in known_clusters.txt."""
    known_file = os.path.join(project_folder, "data", "known_clusters.txt")
    if not os.path.exists(known_file):
        return set()
    with open(known_file, "r") as f:
        fcntl.flock(f, fcntl.LOCK_SH)
        clusters = {line.strip() for line in f if line.strip()}
        fcntl.flock(f, fcntl.LOCK_UN)
    return clusters

def add_known_cluster(project_folder, cluster_name):
    """Append the new cluster name to known_clusters.txt."""
    known_file = os.path.join(project_folder, "data", "known_clusters.txt")
    os.makedirs(os.path.dirname(known_file), exist_ok=True)
    with open(known_file, "a") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        f.write(f"{cluster_name}\n")
        fcntl.flock(f, fcntl.LOCK_UN)

def confirm_new_cluster(cluster_name, known_clusters):
    """Prompt the user to confirm the use of a new cluster name."""
    if known_clusters:
        print("Known clusters: " + ", ".join(sorted(known_clusters)))
    else:
        print("No known clusters yet.")
    response = input(f"Cluster name '{cluster_name}' is new. Are you sure you want to poll a new cluster? (y/n): ")
    return response.strip().lower().startswith("y")

def get_squeue_jobs():
    """
    Calls squeue and returns a dictionary mapping username -> set(job_ids).
    Uses the -h flag (no header) and the format options to output jobid and user.
    """
    cmd = ["squeue", "-h", "-o", "%i %u"]
    result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    if result.returncode != 0:
        print("Error calling squeue:", result.stderr)
        return {}
    
    jobs_by_user = {}
    for line in result.stdout.strip().splitlines():
        parts = line.split()
        if len(parts) < 2:
            continue
        job_id, user = parts[0], parts[1]
        jobs_by_user.setdefault(user, set()).add(job_id)
    return jobs_by_user

def read_job_ids_from_file(filepath):
    """
    Reads a file that contains one job ID per line.
    Returns a set of job IDs.
    """
    if not os.path.exists(filepath):
        return set()
    with open(filepath, "r") as f:
        fcntl.flock(f, fcntl.LOCK_SH)
        lines = f.readlines()
        fcntl.flock(f, fcntl.LOCK_UN)
    return {line.strip() for line in lines if line.strip()}

def write_job_ids_to_file(filepath, job_ids):
    """
    Overwrites the file with the provided job IDs (one per line).
    """
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, "w") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        for job_id in sorted(job_ids):
            f.write(f"{job_id}\n")
        fcntl.flock(f, fcntl.LOCK_UN)

def append_job_ids_to_file(filepath, job_ids):
    """
    Appends the provided job IDs (one per line) to the file.
    """
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, "a") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        for job_id in sorted(job_ids):
            f.write(f"{job_id}\n")
        fcntl.flock(f, fcntl.LOCK_UN)

def process_user_jobs(project_folder, cluster_name, user, current_jobs_set):
    """
    For a given user, compare the new set of current jobs (from squeue)
    with what was stored on disk. If any jobs have dropped out, append them
    to the finished jobs file. Then, update the current jobs file.
    """
    current_jobs_folder = os.path.join(project_folder, "data", user, "current_jobs")
    os.makedirs(current_jobs_folder, exist_ok=True)

    finished_jobs_folder = os.path.join(project_folder, "data", user, "finished_jobs")
    os.makedirs(finished_jobs_folder, exist_ok=True)

    current_jobs_file  = os.path.join(project_folder, "data", user, "current_jobs",  f"{cluster_name}_jobs.txt")
    finished_jobs_file = os.path.join(project_folder, "data", user, "finished_jobs", f"{cluster_name}_jobs.txt")
    
    previous_jobs = read_job_ids_from_file(current_jobs_file)
    finished_jobs = previous_jobs - current_jobs_set  # Jobs that are no longer running.
    
    if finished_jobs:
        append_job_ids_to_file(finished_jobs_file, finished_jobs)
    
    # Update the current jobs file with the new set.
    write_job_ids_to_file(current_jobs_file, current_jobs_set)

def monitor_squeue(project_folder, cluster_name):
    while True:
        jobs_by_user = get_squeue_jobs()
        for user, current_jobs in jobs_by_user.items():
            process_user_jobs(project_folder, cluster_name, user, current_jobs)
        time.sleep(POLL_INTERVAL)

def main():
    parser = argparse.ArgumentParser(description="SLURM squeue monitor daemon")
    parser.add_argument("cluster", help="Name of the cluster (e.g., CMS, Hammer, AF, Geddes, etc.)")
    parser.add_argument("--project-folder", default="/depot/cms/top/awildrid/SlurmScoreboard",
                        help="Path to the project folder (default: /depot/cms/top/awildrid/SlurmScoreboard)")
    args = parser.parse_args()
    
    cluster_name = args.cluster.strip()
    project_folder = args.project_folder.rstrip("/")
    
    # Check for known clusters:
    known_clusters = get_known_clusters(project_folder)
    if cluster_name not in known_clusters:
        if not confirm_new_cluster(cluster_name, known_clusters):
            print("Exiting daemon. Please re-run with the correct cluster name.")
            return
        else:
            add_known_cluster(project_folder, cluster_name)
            print(f"Cluster '{cluster_name}' added to known clusters.")
    else:
        print(f"Using known cluster name: {cluster_name}")
    
    # Start the monitoring loop.
    monitor_squeue(project_folder, cluster_name)

if __name__ == "__main__":
    main()

#!/usr/bin/env python3
import os
import subprocess
import time
import csv
import fcntl
import argparse

# Configuration: Adjust these as needed.
PROCESS_INTERVAL = 3600  # seconds (1 hour)

def get_users(project_folder):
    """
    Returns a list of usernames by scanning the data directory.
    Assumes user directories exist under project_folder/data.
    """
    data_dir = os.path.join(project_folder, "data")
    if not os.path.exists(data_dir):
        return []
    return [entry for entry in os.listdir(data_dir)
            if os.path.isdir(os.path.join(data_dir, entry))]

def read_job_ids_from_file(filepath):
    """
    Reads a file containing job IDs (one per line) and returns a set.
    """
    if not os.path.exists(filepath):
        return set()
    with open(filepath, "r") as f:
        fcntl.flock(f, fcntl.LOCK_SH)
        lines = f.readlines()
        fcntl.flock(f, fcntl.LOCK_UN)
    return {line.strip() for line in lines if line.strip()}

def read_processed_job_ids(processed_csv_file):
    """
    Reads the processed CSV file and returns a set of job IDs that have already been processed.
    """
    processed = set()
    if not os.path.exists(processed_csv_file):
        return processed
    with open(processed_csv_file, "r") as f:
        fcntl.flock(f, fcntl.LOCK_SH)
        reader = csv.DictReader(f)
        for row in reader:
            processed.add(row.get("JobID"))
        fcntl.flock(f, fcntl.LOCK_UN)
    return processed

def append_to_csv(processed_csv_file, rows, fieldnames):
    """
    Appends rows (a list of dictionaries) to the processed CSV file.
    Creates the file and writes headers if it does not exist.
    """
    os.makedirs(os.path.dirname(processed_csv_file), exist_ok=True)
    file_exists = os.path.exists(processed_csv_file)
    with open(processed_csv_file, "a", newline="") as csvfile:
        fcntl.flock(csvfile, fcntl.LOCK_EX)
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        for row in rows:
            writer.writerow(row)
        fcntl.flock(csvfile, fcntl.LOCK_UN)

def parse_sacct_output(job_id, cluster_name):
    """
    Calls sacct for the given job ID and parses the output to extract
    details: JobID, User, Elapsed, MaxRSS, AllocCPUS, and either ReqGRES or ReqTRES.
    Uses pipe-separated output (-P) with no header (-n).

    Returns a dict with additional fields:
      - GPU_Flag (True/False as string)
      - ResourceCount (number of GPUs if GPU job, else AllocCPUS)
      - Cluster (the CLUSTER_NAME)
    """
    def run_sacct(fields):
        cmd = ["sacct", "-j", job_id, "-n", "-P", "-o", fields]
        return subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)

    # First try with ReqGRES (for older SLURM installations)
    fields = "JobID,User,Elapsed,MaxRSS,AllocCPUS,ReqGRES"
    result = run_sacct(fields)
    used_field = "ReqGRES"

    # If we get an error indicating that ReqGRES is no longer available, try ReqTRES
    if result.returncode != 0 and "ReqGRES has been removed" in result.stderr:
        fields = "JobID,User,Elapsed,MaxRSS,AllocCPUS,ReqTRES"
        result = run_sacct(fields)
        used_field = "ReqTRES"
        if result.returncode != 0:
            print(f"Error calling sacct for job {job_id} with ReqTRES: {result.stderr}")
            return None

    if not result.stdout.strip():
        return None

    # Use the first (usually the main job record) line from sacct
    line = result.stdout.strip().splitlines()[0]
    parts = line.split("|")
    if len(parts) < 6:
        return None

    # For consistency, store the requested resource field under the same key.
    job_req = parts[5]
    job_info = {
        "JobID": parts[0],
        "User": parts[1],
        "Elapsed": parts[2],
        "MaxRSS": parts[3],
        "AllocCPUS": parts[4],
        "ReqGRES": job_req,  # even if it actually came from ReqTRES
    }

    # Determine if GPUs were requested.
    req_field = job_req.strip().lower()
    if req_field and "gpu" in req_field:
        job_info["GPU_Flag"] = "True"
        try:
            # The field might be formatted as "gpu:2" or "gpu=2". Try both.
            if ":" in req_field:
                parts_split = req_field.split(":")
            elif "=" in req_field:
                parts_split = req_field.split("=")
            else:
                parts_split = [req_field]
            gpu_count = int(parts_split[1]) if len(parts_split) > 1 else 1
        except Exception:
            gpu_count = 1
        job_info["ResourceCount"] = str(gpu_count)
    else:
        job_info["GPU_Flag"] = "False"
        job_info["ResourceCount"] = job_info["AllocCPUS"]

    # Assuming CLUSTER_NAME is defined (for example via an environment variable)
    job_info["Cluster"] = cluster_name
    return job_info

def process_finished_jobs(cluster_name, project_folder):
    """
    For each user, read the finished jobs file and process any new job IDs
    (those not already in the processed CSV). For each new job, call sacct to
    retrieve job details and append them to the processed CSV.
    """
    users = get_users(project_folder)
    for user in users:        
        finished_jobs_file = os.path.join(project_folder, "data", user, "finished_jobs", f"{cluster_name}_jobs.txt")

        processed_jobs_folder = os.path.join(project_folder, "data", user, "processed_jobs")
        os.makedirs(processed_jobs_folder, exist_ok=True)

        processed_csv_file  = os.path.join(project_folder, "data", user, "processed_jobs", f"{cluster_name}.csv")
        
        finished_job_ids  = read_job_ids_from_file(finished_jobs_file)
        processed_job_ids = read_processed_job_ids(processed_csv_file)
        
        # Only process jobs that haven't been processed yet.
        to_process = finished_job_ids - processed_job_ids
        if not to_process:
            continue
        
        rows = []
        for job_id in to_process:
            job_info = parse_sacct_output(job_id, cluster_name)
            if job_info:
                rows.append(job_info)
        
        if rows:
            fieldnames = ["JobID", "User", "Elapsed", "MaxRSS", "AllocCPUS", "ReqGRES", "GPU_Flag", "ResourceCount", "Cluster"]
            append_to_csv(processed_csv_file, rows, fieldnames)

def processing_loop():
    parser = argparse.ArgumentParser(description="SLURM squeue monitor daemon")
    parser.add_argument("cluster", help="Name of the cluster (e.g., CMS, Hammer, AF, Geddes, etc.)")
    parser.add_argument("--project-folder", default="/depot/cms/top/awildrid/SlurmScoreboard",
                        help="Path to the project folder (default: /depot/cms/top/awildrid/SlurmScoreboard)")
    args = parser.parse_args()

    cluster_name = args.cluster.strip()
    project_folder = args.project_folder.rstrip("/")

    while True:
        process_finished_jobs(cluster_name, project_folder)
        time.sleep(PROCESS_INTERVAL)

if __name__ == "__main__":
    processing_loop()

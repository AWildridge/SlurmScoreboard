# SlurmScoreboard

**SlurmScoreboard** is a terminal-based tool for aggregating and displaying SLURM usage statistics across multiple clusters/backends. It provides a centralized, real-time scoreboard of job runtimes, memory usage, and GPU usage by continuously monitoring SLURM's `squeue` and `sacct` outputs.

## Features

- **Real-time Monitoring:**  
  A daemon that polls `squeue` every 30 seconds to track running jobs and detect when jobs finish.

- **Job Processing:**  
  A separate daemon that periodically (e.g., hourly) processes finished jobs using `sacct` to extract detailed metrics such as elapsed time, maximum memory used, and GPU usage.

- **Scoreboard Builder:**  
  A daily daemon aggregates processed job data into a high score CSV file, keeping the scoreboard current.

- **Interactive and Flexible Viewer:**  
  The scoreboard viewer supports multiple display modes:
  - **Rich Mode (default):** Uses the [rich](https://rich.readthedocs.io) library for a colorful, static display.
  - **Interactive Curses Mode (fallback):** Provides a dynamic, keyboard-interactive UI (sortable by clock, memory, or GPU hours) if rich isn’t installed.
  - **Plain Text Mode:** A simple ASCII table (top 100 users sorted by clock hours) for maximum compatibility.

- **Cross-Cluster Support:**  
  Easily configured for multiple clusters through command-line arguments. The system validates cluster names to avoid typos or ambiguous naming.

## Installation

Ensure you have Python 3 installed. While the project works with Python 3.6+, some features (e.g., `capture_output` in `subprocess.run`) require a workaround for older versions.

Optional dependency for enhanced display:

    pip install rich

Clone the repository:

    git clone https://github.com/yourusername/SlurmScoreboard.git
    cd SlurmScoreboard

## Usage

### 1. squeue Monitor Daemon

Run this daemon on your cluster's front-end node. It polls `squeue` every 30 seconds and logs current and finished jobs:

    ./squeue_monitor.py <cluster_name> --project-folder /path/to/project_folder

*Example:*

    ./squeue_monitor.py Hammer --project-folder /depot/cms/top/awildrid/my_project

### 2. sacct Processor Daemon

This daemon should run periodically (e.g., via cron or a service) to process finished jobs and generate detailed usage statistics:

    ./sacct_processor.py --project-folder /path/to/project_folder

### 3. Scoreboard Builder Daemon

Aggregates processed job data once a day and creates a consolidated scoreboard CSV file:

    ./scoreboard_builder.py --project-folder /path/to/project_folder --scoreboard-file /path/to/scoreboard.csv

### 4. Scoreboard Viewer

Users can view the scoreboard using one of three modes:

- **Default (Rich) Mode:**  
  If the `rich` library is installed, run:

      ./scoreboard_viewer.py --scoreboard-file /path/to/scoreboard.csv

- **Interactive Curses Mode (Fallback):**  
  If `rich` is not installed and you do not specify `--plaintext`, an interactive UI will launch. Use:
  - **c** to sort by Clock Hours
  - **m** to sort by Memory Hours
  - **g** to sort by GPU Hours
  - **q** to quit

- **Plain Text Mode:**  
  To display a simple plain-text table (top 100 users sorted by Clock Hours):

      ./scoreboard_viewer.py --plaintext --scoreboard-file /path/to/scoreboard.csv

## Configuration

- **Project Folder:**  
  All data (logs, processed CSVs, scoreboard) are stored under a central project folder (e.g., `/depot/cms/top/awildrid/my_project`).

- **Cluster Naming:**  
  Cluster names are provided as command-line arguments. If a new cluster name is entered, the system lists current known clusters and asks for confirmation before adding it to avoid typos or ambiguous names.

## Compatibility

- **Python:**  
  Supports Python 3.6 and above (with fallbacks for features introduced in later versions).

- **SLURM:**  
  Designed to work with SLURM environments. Tested across clusters with varying configurations.

## Contributing

Contributions, bug reports, and feature requests are welcome!  
Please open an issue or submit a pull request on [GitHub](https://github.com/yourusername/SlurmScoreboard).

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

## Acknowledgements

Thanks to all the contributors and to the maintainers of SLURM for providing a robust cluster management system.

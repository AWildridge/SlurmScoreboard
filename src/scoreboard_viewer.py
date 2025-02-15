#!/usr/bin/env python3
import os
import csv
import argparse
import sys

# ---------------------------------------------------------------------------
# Plaintext display: a simple ASCII table of the top 100 users sorted by ClockHours.
# ---------------------------------------------------------------------------
def view_scoreboard_plaintext(scoreboard_file):
    if not os.path.exists(scoreboard_file):
        print("Scoreboard file not found:", scoreboard_file)
        return

    data = []
    with open(scoreboard_file, "r") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            try:
                row["_ClockHours"] = float(row.get("ClockHours", "0"))
            except ValueError:
                row["_ClockHours"] = 0.0
            data.append(row)

    # Sort descending by ClockHours and take top 100.
    data.sort(key=lambda r: r["_ClockHours"], reverse=True)
    data = data[:100]

    headers = ["User", "ClockHours", "MemoryHours", "GPUHours"]
    # Determine column widths based on header and data.
    col_widths = {header: len(header) for header in headers}
    for row in data:
        for header in headers:
            col_widths[header] = max(col_widths[header], len(str(row.get(header, ""))))

    header_line = " | ".join(header.ljust(col_widths[header]) for header in headers)
    separator_line = "-+-".join("-" * col_widths[header] for header in headers)

    print(header_line)
    print(separator_line)
    for row in data:
        line = " | ".join(str(row.get(header, "")).ljust(col_widths[header]) for header in headers)
        print(line)

# ---------------------------------------------------------------------------
# Rich display: a static pretty table using rich.
# ---------------------------------------------------------------------------
def view_scoreboard_rich(scoreboard_file):
    from rich.console import Console
    from rich.table import Table

    if not os.path.exists(scoreboard_file):
        print("Scoreboard file not found:", scoreboard_file)
        return

    data = []
    with open(scoreboard_file, "r") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            data.append(row)

    table = Table(title="SLURM High Score Board")
    table.add_column("User", style="cyan", justify="left")
    table.add_column("ClockHours", justify="right")
    table.add_column("MemoryHours", justify="right")
    table.add_column("GPUHours", justify="right")

    # Display all rows; you might sort the data if desired.
    for row in data:
        table.add_row(row.get("User", ""),
                      row.get("ClockHours", ""),
                      row.get("MemoryHours", ""),
                      row.get("GPUHours", ""))
    Console().print(table)

# ---------------------------------------------------------------------------
# Curses Interactive display: an interactive UI for sorting and viewing.
# ---------------------------------------------------------------------------
def interactive_viewer(scoreboard_file):
    import curses

    def load_scoreboard(file_path):
        if not os.path.exists(file_path):
            return []
        data = []
        with open(file_path, "r") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                try:
                    row["_ClockHours"] = float(row.get("ClockHours", "0"))
                    row["_MemoryHours"] = float(row.get("MemoryHours", "0"))
                    row["_GPUHours"] = float(row.get("GPUHours", "0"))
                except ValueError:
                    row["_ClockHours"] = row["_MemoryHours"] = row["_GPUHours"] = 0.0
                data.append(row)
        return data

    def sort_data(data, sort_key):
        # sort_key should be one of "_ClockHours", "_MemoryHours", or "_GPUHours"
        return sorted(data, key=lambda x: x.get(sort_key, 0), reverse=True)

    def display_table(stdscr, data, current_sort):
        stdscr.clear()
        height, width = stdscr.getmaxyx()
        title = ("Interactive SLURM Scoreboard "
                 f"(sorted by {current_sort[1:].capitalize()})  --  "
                 "Press 'c' (Clock), 'm' (Memory), 'g' (GPU) to sort, 'q' to quit")
        stdscr.addstr(0, 0, title[:width], curses.A_BOLD)

        headers = ["User", "ClockHours", "MemoryHours", "GPUHours"]
        col_widths = [max(len(h), 10) for h in headers]

        # Draw header (starting at row 2)
        header_line = " | ".join(h.ljust(col_widths[i]) for i, h in enumerate(headers))
        stdscr.addstr(2, 0, header_line[:width], curses.A_UNDERLINE)

        # Print rows (starting at row 3)
        row_y = 3
        for row in data:
            if row_y >= height - 1:
                break
            line = (f"{row.get('User',''):<{col_widths[0]}} | "
                    f"{row.get('ClockHours',''):<{col_widths[1]}} | "
                    f"{row.get('MemoryHours',''):<{col_widths[2]}} | "
                    f"{row.get('GPUHours',''):<{col_widths[3]}}")
            stdscr.addstr(row_y, 0, line[:width])
            row_y += 1

        stdscr.refresh()

    def curses_main(stdscr):
        curses.curs_set(0)  # hide the cursor
        data = load_scoreboard(scoreboard_file)
        current_sort = "_ClockHours"
        data = sort_data(data, current_sort)
        display_table(stdscr, data, current_sort)
        while True:
            key = stdscr.getch()
            if key == ord('q'):
                break
            elif key == ord('c'):
                current_sort = "_ClockHours"
            elif key == ord('m'):
                current_sort = "_MemoryHours"
            elif key == ord('g'):
                current_sort = "_GPUHours"
            data = sort_data(data, current_sort)
            display_table(stdscr, data, current_sort)

    curses.wrapper(curses_main)

# ---------------------------------------------------------------------------
# Main function: choose display mode based on --plaintext flag and rich availability.
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="SLURM Scoreboard Viewer")
    parser.add_argument("--scoreboard-file", default="/depot/cms/top/awildrid/my_project/scoreboard.csv",
                        help="Path to the scoreboard CSV file")
    parser.add_argument("--plaintext", action="store_true",
                        help="Display a simple plaintext scoreboard (top 100 sorted by ClockHours)")
    args = parser.parse_args()
    scoreboard_file = args.scoreboard_file

    if args.plaintext:
        view_scoreboard_plaintext(scoreboard_file)
    else:
        try:
            # Try to use rich display by default.
            import rich  # noqa: F401
            view_scoreboard_rich(scoreboard_file)
        except ImportError:
            sys.stderr.write("Rich is not installed. For a prettier display, install 'rich'.\n")
            sys.stderr.write("Falling back to an interactive curses view.\n")
            interactive_viewer(scoreboard_file)

if __name__ == "__main__":
    main()

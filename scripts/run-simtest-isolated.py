#!/usr/bin/env python3
# Copyright (c) Walrus Foundation
# SPDX-License-Identifier: Apache-2.0

"""
Wrapper script to run seed-search.py in an isolated process group.
This prevents seed-search.py from killing the parent process when it exits.
"""

import os
import sys
import subprocess
import signal

# Global variable to store the subprocess
proc = None

def signal_handler(signum, frame):
    """Handle signals by forwarding them to the subprocess."""
    global proc
    if proc and proc.poll() is None:
        # Forward the signal to the entire process group of the subprocess
        try:
            os.killpg(os.getpgid(proc.pid), signum)
        except ProcessLookupError:
            pass
    sys.exit(130 if signum == signal.SIGINT else 1)

def main():
    global proc

    if len(sys.argv) < 2:
        print("Usage: run-simtest-isolated.py <seed-search.py arguments>", file=sys.stderr)
        sys.exit(1)

    # Create a new process group
    os.setpgid(0, 0)

    # Set up signal handlers to forward signals to the subprocess
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Run seed-search.py with all the arguments passed to this script
    cmd = ["./scripts/simtest/seed-search.py"] + sys.argv[1:]

    try:
        # Start the subprocess in a new process group
        proc = subprocess.Popen(cmd, preexec_fn=os.setsid)

        # Wait for the subprocess to complete
        returncode = proc.wait()
        sys.exit(returncode)
    except KeyboardInterrupt:
        # Handle Ctrl+C
        if proc and proc.poll() is None:
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGINT)
                proc.wait(timeout=5)
            except:
                proc.kill()
        sys.exit(130)
    except Exception as e:
        print(f"Error running seed-search.py: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()

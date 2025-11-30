#!/bin/zsh

# Absolute path to your project directory
PROJECT_DIR="/Users/fergalclarke/matchday v2"
LOG_DIR="$PROJECT_DIR/logs"
mkdir -p "$LOG_DIR"

# Use the same python3 you use in the terminal
PYTHON_BIN="/usr/local/bin/python3"  # adjust after running `which python3`

# Log file with date
NOW=$(date +"%Y-%m-%d_%H-%M-%S")
LOGFILE="$LOG_DIR/sync_$NOW.log"

{
  echo "==== Matchday sync started at $(date) ===="

  cd "$PROJECT_DIR" || exit 1

  echo "[INFO] Running football sync..."
  $PYTHON_BIN sync_fixtures_to_airtable.py

  echo "[INFO] Running rugby sync..."
  $PYTHON_BIN sync_rugby_to_airtable.py

  echo "[INFO] Running GAA sync..."
  $PYTHON_BIN sync_gaa_to_airtable.py

  echo "==== Matchday sync finished at $(date) ===="
} >> "$LOGFILE" 2>&1

import subprocess
import sys
from pathlib import Path

# Absolute path to your matchday folder
BASE_DIR = Path("/Users/fergalclarke/matchday v2")

SCRIPTS = [
    "sync_fixtures_to_airtable.py",
    "sync_rugby_to_airtable.py",
    "sync_gaa_to_airtable.py",
]

def run_script(script_name):
    script_path = BASE_DIR / script_name

    print(f"\n=== Running {script_name} ===")

    result = subprocess.run(
        [sys.executable, str(script_path)],
        capture_output=True,
        text=True
    )

    print(result.stdout)
    if result.stderr:
        print("ERROR:", result.stderr)


def main():
    print("=== Starting Full Sync ===")
    for script in SCRIPTS:
        run_script(script)
    print("\n=== All Scripts Complete ===")


if __name__ == "__main__":
    main()

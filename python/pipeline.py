from run_cleaning import run_cleaning
from validation import run_validation  # make a run_validation() that checks all cleaned files
from load_to_db import load_to_db

def run_pipeline() -> None:
    # Step 1: If raw already exists, do nothing here for now.
    print("Raw data assumed present in data/raw/")

    # Step 2: Clean
    run_cleaning()

    # Step 3: Validate
    run_validation()

    # Step 4: load to db
    load_to_db()

    print("Pipeline complete.")

if __name__ == "__main__":
    run_pipeline()

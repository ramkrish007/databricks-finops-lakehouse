import csv
import os
import random
from datetime import datetime, timedelta

from faker import Faker

from config import (
    NUM_DEPARTMENTS,
    NUM_DEPARTMENT_CHANGES,
    OUTPUT_INITIAL_PATH,
    DEPARTMENTS,
    LOCATIONS
)

# Faker is used to generate realistic names and emails
fake = Faker()

# Create output directory if it does not exist
os.makedirs(OUTPUT_INITIAL_PATH, exist_ok=True)


def generate_departments():
    """
    Generate master department data.

    Output:
        departments.csv

    Purpose:
        - Source for dim_department
        - Used later for broadcast joins
        - Supports budget analysis
    """

    departments = []

    for i in range(NUM_DEPARTMENTS):

        departments.append({
            "department_id": f"D{i + 1:03}",
            "department_name": DEPARTMENTS[i],
            "manager_name": fake.name(),
            "manager_email": fake.email(),
            "monthly_budget": random.randint(25000, 250000),
            "location": random.choice(LOCATIONS),
            "created_date": "2026-01-01"
        })

    file_path = os.path.join(
        OUTPUT_INITIAL_PATH,
        "departments.csv"
    )

    with open(file_path, "w", newline="") as file:

        writer = csv.DictWriter(
            file,
            fieldnames=departments[0].keys()
        )

        writer.writeheader()
        writer.writerows(departments)

    print(f"Generated: {file_path}")

    return departments


def generate_department_changes(departments):
    """
    Generate department changes.

    Output:
        department_changes.csv

    Purpose:
        - Source for SCD Type 2 implementation
        - Simulates manager changes
        - Simulates budget changes
        - Used later with MERGE INTO
    """

    changes = []

    for _ in range(NUM_DEPARTMENT_CHANGES):

        dept = random.choice(departments)

        # Generate random change date during 2026
        change_date = (
            datetime(2026, 1, 1)
            + timedelta(days=random.randint(1, 365))
        )

        changes.append({
            "department_id": dept["department_id"],
            "department_name": dept["department_name"],

            # New manager assignment
            "manager_name": fake.name(),

            # Budget change event
            "monthly_budget": random.randint(
                25000,
                300000
            ),

            "effective_date": change_date.strftime(
                "%Y-%m-%d"
            )
        })

    file_path = os.path.join(
        OUTPUT_INITIAL_PATH,
        "department_changes.csv"
    )

    with open(file_path, "w", newline="") as file:

        writer = csv.DictWriter(
            file,
            fieldnames=changes[0].keys()
        )

        writer.writeheader()
        writer.writerows(changes)

    print(f"Generated: {file_path}")


def main():
    """
    Main execution flow.

    Step 1:
        Generate department master data

    Step 2:
        Generate department change history

    Future:
        Additional generators will create:
            - cloud_usage.json
            - software_licenses.csv
            - incidents.json
    """

    departments = generate_departments()

    generate_department_changes(
        departments
    )

    print("Initial load files generated successfully")


if __name__ == "__main__":
    main()
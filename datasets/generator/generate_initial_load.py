import csv
import json
import os
import random
from datetime import datetime, timedelta

from faker import Faker

from config import (
    NUM_DEPARTMENTS,
    NUM_DEPARTMENT_CHANGES,
    NUM_CLOUD_RECORDS,
    OUTPUT_INITIAL_PATH,
    DEPARTMENTS,
    LOCATIONS,
    SERVICES,
    REGIONS,
    ENVIRONMENTS,
    INSTANCE_TYPES,
    NUM_SOFTWARE_RECORDS,
    SOFTWARE_PRODUCTS
)

# Faker is used to generate realistic names and emails
fake = Faker()

SOFTWARE_PRICING = {
    "Jira": 120,
    "Confluence": 100,
    "Power BI": 150,
    "Tableau": 900,
    "Datadog": 600,
    "Snowflake": 1200,
    "ServiceNow": 1500,
    "GitHub Enterprise": 250
}

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

def generate_cloud_usage():
    """
    Generate cloud usage data.

    Output:
        cloud_usage_2026_01.json
        cloud_usage_2026_02.json
        cloud_usage_2026_03.json
        cloud_usage_2026_04.json
        cloud_usage_2026_05.json

    Concepts Covered:
        - Nested JSON
        - Arrays
        - Explode
        - Data Type Conversion
        - Duplicate Records
        - Quarantine Records
        - Schema Evolution
        - Underutilized Resources
    """

    records_per_month = NUM_CLOUD_RECORDS // 5

    for month in range(1, 6):

        cloud_records = []

        for i in range(records_per_month):

            resource_id = f"RES-{month}-{i:06}"

            # 20% underutilized resources
            if random.random() < 0.20:
                cpu_utilization = str(random.randint(1, 19))
            else:
                cpu_utilization = str(random.randint(20, 95))

            record = {
                "resource_id": resource_id,
                "department_id": f"D{random.randint(1,20):03}",
                "service": random.choice(SERVICES),
                "region": random.choice(REGIONS),
                "environment": random.choice(ENVIRONMENTS),
                "instance_type": random.choice(INSTANCE_TYPES),

                # Stored as strings intentionally
                "monthly_cost": str(
                    round(random.uniform(100, 5000), 2)
                ),

                "cpu_utilization": cpu_utilization,

                "memory_utilization": str(
                    random.randint(10, 95)
                ),

                "usage_date": f"2026-{month:02}-{random.randint(1,28):02}",

                "tags": [
                    {
                        "key": "Environment",
                        "value": random.choice(
                            ENVIRONMENTS
                        )
                    },
                    {
                        "key": "Owner",
                        "value": f"Team{random.randint(1,10)}"
                    }
                ]
            }

            # 1% schema drift records
            if random.random() < 0.01:

                if random.random() < 0.50:
                    record["project_name"] = (
                        f"Project_{random.randint(1,20)}"
                    )
                else:
                    record["migration_wave"] = (
                        f"Wave_{random.randint(1,5)}"
                    )

            # 1% bad records
            if random.random() < 0.01:

                if random.random() < 0.50:
                    record["monthly_cost"] = "ABC"
                else:
                    record["cpu_utilization"] = "N/A"

            cloud_records.append(record)

        # Create duplicate records (~1%)
        duplicates = random.sample(
            cloud_records,
            int(len(cloud_records) * 0.01)
        )

        cloud_records.extend(duplicates)

        file_path = os.path.join(
            OUTPUT_INITIAL_PATH,
            f"cloud_usage_2026_{month:02}.json"
        )

        with open(file_path, "w") as file:

            for record in cloud_records:
                file.write(
                    json.dumps(record) + "\n"
                )

        print(
            f"Generated: {file_path} "
            f"({len(cloud_records)} records)"
        )

def generate_software_licenses():
    """
    Generate software license data.

    Output:
        software_licenses.csv

    Purpose:
        - License optimization analysis
        - Broadcast join demo
        - Cost saving opportunities
        - Gold KPI calculations
    """

    software_records = []

    for i in range(NUM_SOFTWARE_RECORDS):

        software = random.choice(
            SOFTWARE_PRODUCTS
        )

        licenses_purchased = random.randint(
            10,
            500
        )

        # Intentionally create waste
        if random.random() < 0.30:

            active_users = random.randint(
                1,
                max(1, int(
                    licenses_purchased * 0.40
                ))
            )

        else:

            active_users = random.randint(
                int(
                    licenses_purchased * 0.70
                ),
                licenses_purchased
            )

        annual_cost = (
            licenses_purchased *
            SOFTWARE_PRICING[software]
        )

        software_records.append({
            "license_id": f"LIC-{i+1:06}",
            "software_name": software,
            "department_id": f"D{random.randint(1,20):03}",
            "licenses_purchased": licenses_purchased,
            "active_users": active_users,
            "annual_cost": annual_cost,
            "license_type": random.choice(
                [
                    "Named User",
                    "Concurrent",
                    "Enterprise"
                ]
            ),
            "renewal_date": (
                datetime(
                    2026,
                    random.randint(1, 12),
                    random.randint(1, 28)
                ).strftime("%Y-%m-%d")
            ),
            "vendor": software
        })

    file_path = os.path.join(
        OUTPUT_INITIAL_PATH,
        "software_licenses.csv"
    )

    with open(
        file_path,
        "w",
        newline=""
    ) as file:

        writer = csv.DictWriter(
            file,
            fieldnames=software_records[0].keys()
        )

        writer.writeheader()
        writer.writerows(
            software_records
        )

    print(
        f"Generated: {file_path}"
    )

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

    generate_cloud_usage()

    generate_software_licenses()

    print("Initial load files generated successfully")


if __name__ == "__main__":
    main()
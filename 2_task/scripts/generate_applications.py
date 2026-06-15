import csv
import random
from datetime import datetime, timedelta
import os

random.seed(42)

output_file = "applications.csv"
target_rows = 520000

regions = ["DEHE", "DEBW", "DEBY", "NLFL", "NLNH", "RUMSK", "RUSPB", "PLMA", "FRIDF", "ITLAZ"]
products = ["cash_loan", "credit_card", "installment", "car_loan", "mortgage", "refinance"]
channels = ["mobile", "web", "branch", "call_center"]
risk_levels = ["low", "medium", "high"]
decision_statuses = ["approved", "rejected", "manual_review"]

start_date = datetime(2026, 5, 1, 0, 0, 0)
end_date = datetime(2026, 5, 31, 23, 59, 59)
delta_seconds = int((end_date - start_date).total_seconds())

with open(output_file, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow([
        "application_id", "event_time", "customer_id", "region_code", "product_type",
        "requested_amount", "term_months", "credit_score", "risk_level", "decision_status",
        "approved_amount", "channel", "employee_review_flag", "processing_time_sec"
    ])

    for i in range(1, target_rows + 1):
        event_time = start_date + timedelta(seconds=random.randint(0, delta_seconds))
        requested_amount = random.choice([5000, 8000, 12000, 15000, 25000, 40000, 60000, 90000])
        term_months = random.choice([6, 12, 18, 24, 36, 48, 60])
        credit_score = random.randint(300, 850)
        risk_level = random.choices(risk_levels, weights=[0.5, 0.35, 0.15], k=1)[0]
        decision_status = random.choices(decision_statuses, weights=[0.45, 0.4, 0.15], k=1)[0]

        if decision_status == "approved":
            approved_amount = requested_amount
            employee_review_flag = "false"
            processing_time_sec = random.randint(5, 40)
        elif decision_status == "manual_review":
            approved_amount = random.randint(int(requested_amount * 0.5), requested_amount)
            employee_review_flag = "true"
            processing_time_sec = random.randint(30, 180)
        else:
            approved_amount = 0
            employee_review_flag = "true"
            processing_time_sec = random.randint(5, 90)

        writer.writerow([
            f"app_202605{i:06d}",
            event_time.strftime("%Y-%m-%d %H:%M:%S"),
            f"cust_{random.randint(10000, 99999)}",
            random.choice(regions),
            random.choice(products),
            requested_amount,
            term_months,
            credit_score,
            risk_level,
            decision_status,
            approved_amount,
            random.choice(channels),
            employee_review_flag,
            processing_time_sec
        ])

print(f"Created {output_file}")
print(f"Size: {os.path.getsize(output_file) / 1024 / 1024:.2f} MB")
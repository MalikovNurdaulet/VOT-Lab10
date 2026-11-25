import csv
import json
import random
from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import dag, task

DATA_DIR = Path("/opt/airflow/data")

PRODUCTS = {
    "Ноутбук": "Техника",
    "Мышь": "Техника",
    "Чай": "Продукты",
    "Кофе": "Продукты",
    "Тетрадь": "Канцтовары",
}


@dag(
    dag_id="sales_etl",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(seconds=30)
    },
    tags=["lab10"],
)
def sales_etl():

    @task
    def extract() -> str:
        """Имитирует выгрузку продаж и сохраняет их в CSV."""
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        path = DATA_DIR / f"raw_sales_{stamp}.csv"

        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["product", "category", "qty", "price"])

            for _ in range(50):
                name = random.choice(list(PRODUCTS))
                writer.writerow([
                    name,
                    PRODUCTS[name],
                    random.randint(1, 5),
                    random.randint(500, 50000)
                ])

        print(f"Сохранено 50 строк в {path}")
        return str(path)

    @task
    def transform(raw_path: str) -> dict:
        """Считает выручку (qty * price) по категориям."""
        totals = {}

        with open(raw_path, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                revenue = int(row["qty"]) * int(row["price"])
                totals[row["category"]] = (
                    totals.get(row["category"], 0) + revenue
                )

        print("Выручка по категориям:", totals)
        return totals

    @task
    def load(totals: dict, raw_path: str) -> None:
        """Сохраняет отчёт в JSON рядом с исходным файлом."""
        report = Path(raw_path).with_name(
            Path(raw_path).name.replace("raw_sales", "report")
        ).with_suffix(".json")

        with open(report, "w", encoding="utf-8") as f:
            json.dump(totals, f, ensure_ascii=False, indent=2)

        print(f"Отчёт сохранён: {report}")

    raw = extract()
    load(transform(raw), raw)


sales_etl()

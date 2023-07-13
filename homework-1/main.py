"""Скрипт для заполнения данными таблиц в БД Postgres."""
import csv
import binary as binary
import psycopg2
from pathlib import Path

path = Path('north_data/')
conn = psycopg2.connect(host='localhost', database='north', user='postgres', password='852799')
try:
    with conn:
        with conn.cursor() as cur:
            with open(path/'customers_data.csv', 'r', encoding='UTF-8') as file:
                reader = csv.reader(file)
                next(reader)
                for row in reader:
                    cur.execute("INSERT INTO customers VALUES (%s, %s, %s)",
                                (row[0], row[1], row[2])
                                )

            with open(path/'employees_data.csv', 'r', encoding='UTF-8') as file:
                reader = csv.reader(file)
                next(reader)
                for row in reader:
                    cur.execute("INSERT INTO employees VALUES (%s, %s, %s, %s, %s, %s)",
                                (row[0], row[1], row[2], row[3], row[4], row[5])
                                )

            with open(path/'orders_data.csv', 'r', encoding='UTF-8') as file:
                reader = csv.reader(file)
                next(reader)
                for row in reader:
                    cur.execute("INSERT INTO orders VALUES (%s, %s, %s, %s, %s)",
                                (row[0], row[1], row[2], row[3], row[4])
                                )
finally:
    conn.close()

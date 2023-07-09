"""Скрипт для заполнения данными таблиц в БД Postgres."""
import psycopg2
import csv

conn = psycopg2.connect(host='localhost', database='north', user='postgres', password='dimasolo100700dima')
try:
    with conn:
        with conn.cursor() as cur:
            with open('north_data\customers_data.csv', 'r', encoding='UTF-8') as file:
                file_reader = csv.reader(file)
                next(file_reader)
                for row in file_reader:
                    cur.execute("INSERT INTO customers VALUES (%s, %s, %s)",
                                (row[0], row[1], row[2])
                                )

            with open('north_data\employees_data.csv', 'r', encoding='UTF-8') as file:
                file_reader = csv.reader(file)
                next(file_reader)
                for row in file_reader:
                    cur.execute("INSERT INTO employees VALUES (%s, %s, %s, %s, %s, %s)",
                                (row[0], row[1], row[2], row[3], row[4], row[5])
                                )

            with open('north_data\orders_data.csv', 'r', encoding='UTF-8') as file:
                file_reader = csv.reader(file)
                next(file_reader)
                for row in file_reader:
                    cur.execute("INSERT INTO orders VALUES (%s, %s, %s, %s, %s)",
                                (row[0], row[1], row[2], row[3], row[4])
                                )
finally:
    conn.close()

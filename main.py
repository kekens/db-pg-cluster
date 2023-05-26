from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta, date
from random import randint, choice

import psycopg2
import csv
import time

db_config = {
    'host': 'localhost',
    'database': 'postgres',
    'user': 'postgres',
    'password': 'mypass',
    'port': '5435'
}

def create_test_subject_area():
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE account (
            id SERIAL PRIMARY KEY,
            label VARCHAR(100),
            code VARCHAR(50),
            clientname VARCHAR(100),
            opendate DATE
        )
    ''')

    cursor.execute('''
        CREATE TABLE balance (
            id SERIAL PRIMARY KEY,
            account_id INTEGER,
            rest_type INTEGER,
            amount DECIMAL(10, 2)
        )
    ''')

    cursor.execute('''
        CREATE TABLE movement (
            id SERIAL PRIMARY KEY,
            account_id INTEGER,
            rest_type INTEGER,
            amount DECIMAL(10, 2)
        )
    ''')

    conn.commit()
    cursor.close()
    conn.close()

def simulate_stress_load():
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    query = "SELECT * FROM your_table"
    iterations = 1000

    with open('execution_times.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Iteration', 'Execution Time (ms)'])

        for i in range(iterations):
            start_time = time.time()

            cursor.execute(query)

            end_time = time.time()
            execution_time = (end_time - start_time) * 1000

            writer.writerow([i+1, execution_time])

    cursor.close()
    conn.close()

def generate_random_date(start_date, end_date):
    time_between_dates = end_date - start_date
    days_between_dates = time_between_dates.days
    random_number_of_days = randint(0, days_between_dates)
    random_date = start_date + timedelta(days=random_number_of_days)
    return random_date

def write_random_rows(csv_writer):
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    j = -1
    while True:
        j += 1
        for _ in range(100):
            label = f'Account-{randint(1, 100)}'
            code = f'Code-{randint(1000, 9999)}'
            clientname = f'Client-{randint(1, 50)}'
            opendate = generate_random_date(date(2022, 1, 1), date(2023, 12, 31))

            if _ == 99:
                start_time = time.time()

            cursor.execute("INSERT INTO account (label, code, clientname, opendate) VALUES (%s, %s, %s, %s)",
                           (label, code, clientname, opendate))

            account_id = cursor.lastrowid  # Get the generated account ID

            rest_type = choice([0, 1, 2])
            amount = round(randint(100, 10000) / 100, 2)

            cursor.execute("INSERT INTO balance (account_id, rest_type, amount) VALUES (%s, %s, %s)",
                           (account_id, rest_type, amount))

            rest_type = choice([0, 1, 2])
            amount = round(randint(100, 10000) / 100, 2)

            cursor.execute("INSERT INTO movement (account_id, rest_type, amount) VALUES (%s, %s, %s)",
                           (account_id, rest_type, amount))

            if _ == 99:
                end_time = time.time()
                execution_time = (end_time - start_time) * 1000
                csv_writer.writerow([j + 1, execution_time])

        conn.commit()

    cursor.close()
    conn.close()

if __name__ == '__main__':
    executor = ThreadPoolExecutor(max_workers=8)

    csv_file = open('out/execution_write_times.csv', 'w', newline='')
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow(['Query', 'Execution Time (ms)'])
    for i in range(1,8):
        executor.submit(write_random_rows, csv_writer)

    executor.shutdown(wait=True)

    # create_test_subject_area()
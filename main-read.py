import csv
import sys
import time
from threading import current_thread

import psycopg2
from concurrent.futures import ThreadPoolExecutor

db_config = {
    'host': 'localhost',
    'database': 'postgres',
    'user': 'postgres',
    'password': 'mypass',
    'port': int(sys.argv[1])
}

def read_all_data(csv_writer):
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    j = -1
    print('Start reading on' + sys.argv[1])
    while True:
        j += 1
        start_time = time.time()
        cursor.execute("SELECT * FROM movement")
        cursor.execute("SELECT * FROM balance")
        cursor.execute("SELECT * FROM account")

        cursor.execute("SELECT COUNT(*) FROM movement")
        movement_count = cursor.fetchone()
        cursor.execute("SELECT COUNT(*) FROM balance")
        balance_count = cursor.fetchone()
        cursor.execute("SELECT COUNT(*) FROM account")
        account_count = cursor.fetchone()

        end_time = time.time()
        execution_time = (end_time - start_time) * 1000
        if "0_0" in current_thread().name:
            csv_writer.writerow([j + 1, execution_time, movement_count[0] + balance_count[0] + account_count[0]])

    cursor.close()
    conn.close()

    return movement_data

def read_random_data():
    executor = ThreadPoolExecutor(max_workers=8)

    csv_file = open('read_times' + sys.argv[1] + '-' + sys.argv[2] + '.csv', 'w', newline='')
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow(['Query', 'Execution Time (ms)', 'Common count'])

    for i in range(8):
        executor.submit(read_all_data, csv_writer)

    executor.shutdown(wait=True)

if __name__ == '__main__':
    read_random_data()

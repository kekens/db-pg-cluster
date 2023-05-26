import csv
import datetime
import subprocess
import time

db_host = 'localhost'
db_name = 'postgres'
db_user = 'postgres'

instances = ['5433', '5434', '5435']

def execute_pg_isready(port):
    command = ['pg_isready', '-h', db_host, '-p', port, '-d', db_name, '-u', db_user]
    result = subprocess.run(command, capture_output=True, text=True)

    return result.stdout.strip()

if __name__ == '__main__':
    csv_file = open('out/liveness.csv', 'w', newline='')
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow(['Times'] + instances)

    while True:
        result = []
        for i in instances:
            pg_isready_output = execute_pg_isready(i)
            result.append('1' if 'accepting connections' in pg_isready_output else '0')
        csv_writer.writerow([str(datetime.datetime.now().time())] + result)
        time.sleep(1)
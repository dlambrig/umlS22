import pymysql
import csv
import boto3
import configparser
import base64
import os

# get the MySQL connection info and connect
parser = configparser.ConfigParser()
parser.read("pipeline.conf")
hostname = parser.get("mysql_config", "hostname")
port = parser.get("mysql_config", "port")
username = parser.get("mysql_config", "username")
dbname = parser.get("mysql_config", "database")
password = parser.get("mysql_config", "password")

conn = pymysql.connect(host=hostname,
        user=username,
        password=password,
        db=dbname,
        port=int(port))

if conn is None:
  print("Error connecting to the MySQL database")
else:
  print("MySQL connection established!")

m_query = """SELECT *
    FROM Orders;"""
local_filename = "order_extract.csv"

m_cursor = conn.cursor()
m_cursor.execute(m_query)
results = m_cursor.fetchall()

with open(local_filename, 'w') as fp:
  csv_w = csv.writer(fp, delimiter='|')
  csv_w.writerows(results)

fp.close()
m_cursor.close()
conn.close()

USER = os.getenv('USER').strip("\n")
ACCESS = os.getenv('ACCESS').strip("\n")
print(USER)
s3 = boto3.client(
    's3',
    aws_access_key_id=ACCESS,
    aws_secret_access_key=USER)

s3_file = local_filename

s3.upload_file(
    local_filename,
    'UML',
    s3_file)

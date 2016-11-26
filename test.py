import psycopg2 as pg2
import pandas as pd
import os

passwd = os.environ['EX_REDSHIFT_PASS']



con = pg2.connect(dbname='dev', 
                  host='examplecluster.cngdbfmbk6nh.us-west-2.redshift.amazonaws.com',
                  port=5439,
                  user='masteruser',
                  password=passwd)

cur = con.cursor()

#cur.execute("select * from pg_table_def WHERE schemaname='public';")

cur.execute("select distinct schemaname from pg_table_def;")

schemas = cur.fetchall()

tables = {}

for schema in schemas:
    cur.execute("select distinct tablename from pg_table_def where schemaname = %(schema_name)s;", {'schema_name':schema})
    tables[schema[0]] = cur.fetchall()

#res = cur.fetchall()

#for r in res:
#    print r

#cur.execute("select * from users limit 10;")

#users = cur.fetchall()

#for u in users:
#    print u

print tables

cur.close()
con.close()

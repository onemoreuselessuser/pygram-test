import datetime
import gzip
import psycopg2

dw_string = "host='localhost' dbname='etl' user='sa' password='ma$terFOO' port=1433"
dw_pgconn = psycopg2.connect(dw_string)
cur = dw_pgconn.cursor()

with gzip.open('part-00000.gz','r') as fin:
            for line in fin:
                cur.copy_from(line, 'mr_tbl_datapool_client_nf_template')

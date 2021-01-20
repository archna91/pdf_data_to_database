import pandas as pd
import tabula
from tabula import read_pdf
import psycopg2

df = read_pdf("VECF_20201210.pdf", pages="all")
op=tabula.io.convert_into(input_path='VECF_20201210.pdf', output_path='air_data2.csv', output_format='csv', java_options=None, pages='all')

daf = pd.read_csv('air_data2.csv',skipinitialspace=True,delimiter=',', names=('loc','rmk','notam_text','ntm_no'),encoding='cp1252',header=2)
daf_out = daf.replace(',','', regex=True)


daf_out.to_csv("data_new100.csv")

conn = psycopg2.connect(database="airport", user = "postgres", password = "admin", host = "localhost", port = "5432")


cur = conn.cursor()
cur.execute('CREATE TABLE my_data_air_5 ( S_NO varchar(50),LOC varchar(5000),RMK varchar(5000),NOTAM_TEXT varchar(5000),NTN_NO varchar(5000));')
conn.commit()

csv_data = pd.read_csv(r'data_new100.csv')

data_new = pd.DataFrame(csv_data,columns=[ 'loc', 'rmk', 'notam_text', 'ntm_no'])
# data_new.columns


for row in data_new.itertuples():
    cur.execute('''
                INSERT INTO my_data_air_5 (s_no,loc,rmk,notam_text,ntn_no)
                VALUES (%s, %s, %s, %s, %s)
                ''',
                [row[0],row[1],row[2],row[3],row[4]]
                )

conn.commit()
conn.close()


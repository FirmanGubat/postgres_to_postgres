#Package
from sqlalchemy import create_engine
import psycopg2
import pandas

# connect to PostgreSQL Source
s_host = "host"
s_port = "port"
s_dbname = "database_name"
s_name_user = "username"
s_password = "pass"

#extract data from postgresql Source
def extract():
    try:
        src_conn = psycopg2.connect(host=s_host, port=s_port, dbname=s_dbname, user=s_name_user, password=s_password)
        src_cursor = src_conn.cursor()
        # execute query
        src_cursor.execute(""" SELECT table_name FROM information_schema.tables WHERE table_type='BASE TABLE' AND table_schema='your_schema';""")
        src_tables = src_cursor.fetchall()
        for tbl in src_tables:
            #query and load save data to dataframe
            df = pandas.read_sql_query(f'select * FROM "your_schema"."{tbl[0]}"', src_conn)
            load(df, tbl[0])
    except Exception as e:
        print("Data extract error: " + str(e))
    finally:
        src_conn.close()

#load data to postgres target
def load(df, tbl):
    try:
        rows_imported = 0
        engine = create_engine(f'postgresql://username:password@host:port/database_name')
        print(f'importing rows {rows_imported} to {rows_imported + len(df)}... for table {tbl}')
        # save df to postgres
        df.to_sql(f'{tbl}', engine, if_exists='replace', index=True,method='multi',schema='your_schema')
        rows_imported += len(df)
        print("Data imported successful")
    except Exception as e:
        print("Data load error: " + str(e))

# connect to PostgreSQL Target
t_host = "host"
t_port = "port"
t_dbname = "database_name"
t_name_user = "username"
t_password = "password"

#convert colomn to geom
def convert():
    try:
        convert_conn = psycopg2.connect(host=t_host, port=t_port, dbname=t_dbname, user=t_name_user, password=t_password)
        convert_cursor = convert_conn.cursor()
        # execute query
        convert_cursor.execute(""" SELECT table_name FROM information_schema.tables WHERE table_type='BASE TABLE' AND table_schema='your_schema';""")
        convert_tables = convert_cursor.fetchall()
        for tbl in convert_tables:
            convert_conn = psycopg2.connect(host=t_host, port=t_port, dbname=t_dbname, user=t_name_user, password=t_password)
            convert_cursor = convert_conn.cursor()
            convert_cursor.execute(f"""ALTER TABLE "your_schema"."{tbl[0]}" ALTER COLUMN geom TYPE Geometry USING geom::Geometry ;""")
            convert_conn.commit()

    except Exception as e:
        print("Alter Table error: " + str(e))
    finally:
        convert_conn.close()

try:
    #call function
    extract()
    load()
    convert()
except Exception as e:
    print("Error while extracting data: " + str(e))

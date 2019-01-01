import os.path
import ujson
import psycopg2
from io import StringIO
from config import LOCAL_DB_HOST_IP, LOCAL_USER, LOCAL_PASSW, LOCAL_DATABASE


def get_db_description(path='./db_configs.json'):
    with open(path) as f:
        db_description = ujson.load(f)
    return db_description

def write_to_db(df, destination):
    conn = psycopg2.connect(user=LOCAL_USER, password=LOCAL_PASSW,
                            database=LOCAL_DATABASE, host=LOCAL_DB_HOST_IP)

    sio = StringIO()
    sio.write(df.to_csv(index=None, header=None))  # Write the Pandas DataFrame as a csv to the buffer
    sio.seek(0)  # Be sure to reset the position to the start of the stream

    with conn.cursor() as c:
        c.copy_from(sio, "{}".format(destination), columns=df.columns, sep=',', null='')
        conn.commit()

def save_df_dict(df_dict, path='./', suffix=''):
    for key, df in df_dict.items():
        df.to_csv(os.path.join(path, '{0}{1}.csv'.format(key, suffix)), index=None)

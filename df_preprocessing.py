import pandas as pd
import re
from tqdm import tqdm
from normalizephonenumbers import NormalizePhoneNumbers
from sqlalchemy import create_engine
from email.utils import parseaddr
from utils import write_to_db


class DF_Preprocessing():
    """
    Class that connect to source, get data, clean phone and emails, and write table into local DB.
    """
    def __init__(self, db_description: dict):
        self.db_description = db_description

        self.engines = {}
        for key in self.db_description.keys():
            self.engines[key] = self.create_connection(db_description[key]['user'], db_description[key]['pass'],
                                                       db_description[key]['db_name'], db_description[key]['db_ip_port']
                                                       , db_description[key]['scheme_name'])

    def create_connection(self, User, Passw, db_name, db_ip, scheme_name):
        con_description = db_name + '://' + User + ':' + Passw + '@' + db_ip + scheme_name
        connection = create_engine(con_description)
        return connection

    def cleaning_email(self, strr: str):
        if strr is None:
            ret = None
        else:
            ret = parseaddr(strr.replace(' ', '').strip())[1]

            if re.findall('[а-яА-Я\[\],/\\\]', ret.strip()):
                ret = None
        return ret

    def cleaning(self, df):
        # Filtering
        df = df[(df.phone.notnull()) | (df.email.notnull())]

        # Cleaning phone
        phone_normalizer = NormalizePhoneNumbers()
        df['clean_phone'] = df.phone.apply(lambda x: phone_normalizer(str(x)))

        # Cleaning email
        df['clean_email'] = df.email.apply(self.cleaning_email)

        return df

    def __call__(self):
        raw_df_dict = {}
        for key in tqdm(self.db_description.keys()):
            df = pd.read_sql_query(self.db_description[key]['query'].replace("$", '"'), self.engines[key])
            df = self.cleaning(df, key)
            raw_df_dict.update({key: df})
            write_to_db(df,
                        destination=self.db_description[key]['destination'])

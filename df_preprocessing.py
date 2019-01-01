import pandas as pd
import numpy as np
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


    def cleaning_phone(self, strr: str):
        if isinstance(strr, str):
            if re.findall('[A-Za-zа-яА-Я_=\+\.\-\(\)\*/\?\s]', strr.strip()):
                if len(strr) > 25:
                    ret = 12345
                elif '_' in strr:
                    ret = int(strr.split('_')[0])
                elif 'del' in strr:
                    ret = int(strr.split('(')[0])
                elif '+' in strr and len(strr) > 2:
                    ret = int(''.join(re.findall('\d', strr)))
                else:
                    ret = 12345
            elif len(strr) < 1:
                ret = 12345
            else:
                ret = int(strr.strip())

            ret = 12345 if ret.bit_length() > 64 else ret


        elif isinstance(strr, int):
            ret = int(strr)
        elif isinstance(strr, float):
            if np.isnan(strr):
                ret = 123456
            else:
                ret = int(strr)
        else:
            ret = 1234
        return ret

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
        df['clean_phone'] = df['clean_phone'].apply(self.cleaning_phone)

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

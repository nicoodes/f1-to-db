import requests
from pprint import pprint
import pandas as pd
from sqlalchemy import create_engine
import sys
import os

res=requests.get('http://ergast.com/api/f1/constructors.json')

#pprint(res.json())
#pprint(res.json()['MRData']['ConstructorTable']['Constructors'])


df=pd.DataFrame(res.json()['MRData']['ConstructorTable']['Constructors'])

#print(df.head())



# connection to db


HOST_NAME=os.environ["HOST_NAME"]
USER_NAME=os.environ["USER_NAME"]
PASSWORD_DB=os.environ["PASSWORD_DB"]
DATABASE_NAME=os.environ["DATABASE_NAME"]


param_dic2 = f"postgresql+psycopg2://{USER_NAME}:{PASSWORD_DB}@{HOST_NAME}:5432/{DATABASE_NAME}"


def connect(params_dic):
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        #conn = psycopg2.connect(**params_dic)
        conn = create_engine(params_dic)
    except Exception as error:
        print(error)
        sys.exit(1) 
    print("Connection successful")
    return conn
conn = connect(param_dic2)


df.to_sql(name='constructors', schema='public', con=conn, if_exists='replace')


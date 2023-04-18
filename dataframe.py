import requests
import csv
import pandas as pd
import pyspark.sql as psql
from pyspark.sql.types import *
from apikeys import key,host

spark = psql.SparkSession.builder.master("local[2]").appName("pySpark").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

def getresponse():
    url = "https://covid-19-india2.p.rapidapi.com/details.php"

    headers = {
        "X-RapidAPI-Key": key,
        "X-RapidAPI-Host": host
    }
    response = requests.request("GET", url, headers=headers)
    return response

def extract_data(response):
    rawdata=response.json()
    y=rawdata.values()
    mydata=list(y)
    mydata.pop()
    mydata.pop()
    df = pd.DataFrame(mydata)
    # Specify the order of the columns in the DataFrame
    column_order = ['slno', 'state', 'confirm', 'cured', 'death', 'total']
    df = df[column_order]
    clean_data(df)
    df['state'] =df['state'].astype(str)
    df['slno'] = df['slno'].astype(int)
    df['confirm'] = df['confirm'].astype(int)
    df['cured'] = df['cured'].astype(int)
    df['death'] = df['death'].astype(int)
    df['total']=df['total'].astype(int)
    return df
    
def clean_data(df):
    for index,value in df['state'].iteritems():
        if '*' in value:
            value=value.split('*')[0]
            df.loc[index, 'state'] = value
    return df

def create_dataframe(df):
    return spark.createDataFrame(df)

def creating_csvFile(df):
    df.to_csv('my_data.csv', index=False)


response=getresponse()
df=extract_data(response)
creating_csvFile(df)
df=create_dataframe(df)
print(df.show(40))

    

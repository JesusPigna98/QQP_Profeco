from etl.scrapper import get_base_url,extract_files,ensure_folder
from etl.transformations import create_dataframe, merge_dataframes
from db.setup import create_tables, create_db_engine
from db.models import Sales
from db import settings
import requests
from datetime import datetime
import os

class DownloadData:
    def __init__(self,base_url,year,month,file_path):
        self.base_url = base_url
        self.year = year
        self.month = month
        self.file_path = file_path

    def scrape_base_url(self):
        print(f"Scrapping from {self.base_url}")
        self.url = get_base_url(self.base_url,self.year)
    
    def download_files(self):
        print(f"Most recent files found for {self.year}-{self.month}")
        print("Checking if month's folder exists..")
        ensure_folder(self.file_path,self.month)
        print(f"Attempting to download files from {self.url}")

        qqp_url = requests.get(self.url)
        extract_files(qqp_url,self.month,self.file_path)

        print("Download completed")


class MergeDataFrame:

    def __init__(self,year,month,files_dir):
        self.year = year
        self.month = month
        self.files_dir = files_dir

    def get_dataframes(self):
        return create_dataframe(self.files_dir)
    
    def set_dataframes(self,df_list):
        return merge_dataframes(df_list)


class LoadToDB:
    def __init__(self,merged_df):
        self.table_name = Sales.__tablename__
        self.merged_df = merged_df

    def pg_create_tables(self):
        self.engine = create_db_engine()
        create_tables(self.table_name,self.engine)

    def insert_stg_data(self):
        url = 'jdbc:postgresql://{host}:{port}/qqp_2024'.format(**settings.DATABASE['qqp'])
        schema_table = f'"public"."{self.table_name}"'
        properties = {
            'user':settings.DATABASE['qqp']['username'],
            'password':settings.DATABASE['qqp']['password'],
            'driver': 'org.postgresql.Driver'
        }


        print("Trying to insert into table..")
        self.merged_df.write.jdbc(url=url,table=schema_table,properties=properties,mode='overwrite')
        print(f"Done! {merged_df.count()} rows were inserted succesfully.")




        




base_url = 'https://datos.profeco.gob.mx/datos_abiertos/'
year = str(datetime.now().year)
current_month = str(datetime.now().month -1).rjust(2,'0')
files_folder = 'files'
files_dir = os.path.join(os.getcwd(),files_folder,year,current_month)


qqp_extract = DownloadData(base_url,year,current_month,files_dir)
qqp_extract.scrape_base_url()
qqp_extract.download_files()

qqp_transform = MergeDataFrame(year,current_month,files_dir)
df_list = qqp_transform.get_dataframes()
merged_df = qqp_transform.set_dataframes(df_list)


qqp_load = LoadToDB(merged_df)
#qqp_load.pg_create_tables() --Only needed when first created
qqp_load.insert_stg_data()


###Note
## run this commands in container sudo apt-get update & sudo apt-get install unrar
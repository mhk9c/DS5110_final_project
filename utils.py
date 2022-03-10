from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import os
import sys
import subprocess
import pyspark.sql.functions as func
from pyspark.sql.types import StringType, ArrayType
import re
# from pyspark.sql.functions import col,lit

class Tools():          
    def __init__(self, username):
        self.data_path = "/project/ds5559/team1_sp22/data/"  
        self.b = re.compile(r"@[a-zA-Z0-9]+")
        self.spark = SparkSession \
            .builder \
            .master("local") \
            .appName("team1_sp22_final_project") \
            .config("spark.executor.memory", '12g') \
            .config('spark.executor.cores', '2') \
            .config('spark.cores.max', '2') \
            .config("spark.driver.memory",'8g') \
            .getOrCreate()
        
        self.install_custom_packages(username)
    
    def install_custom_packages(self, username='mhk9c'):
        # Simple pattern to Install custom packages from Juypter.
        # username = 'mhk9c'
        # Install a pip package in the current Jupyter kernel        
        
        subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'demoji'])
        subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'tldextract'])
        subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'emoji'])
        
        
        
#         sys.executable -m pip install demoji
#         sys.executable -m pip install tldextract

        sys.path.append(f'/home/{username}/.local/lib/python3.7/site-packages/')
        
        
        
    def save_df(self, _df, folder_name):
        '''
        saves a df to a parquet in some folder.
        '''
        # Check whether the specified path exists or not
        full_path = f'{self.data_path}{folder_name}'
        print(full_path)  
        if not os.path.exists(full_path):  
            # Create a new directory because it does not exist 
            os.makedirs(full_path)
            print("The new directory is created!")

        _df.write.format("parquet").mode("overwrite").save(f"{full_path}")
#         Change the permissions so everyone can use the save.
        os.system(f'chmod -R 777 {full_path}')
        print(f'Saved as: {full_path}')

    
    def load_data(self, folder_name): 
        '''
        Loads a parquet from a folder.
        '''
        full_path = f'{self.data_path}/{folder_name}'
        _df = self.spark.read.parquet(full_path)        
        print(f'Done loading from {full_path}.')
        return _df


    def create_df_from_csv(self, folder_name):
        '''
        Assumes data is saved as individual csv's in a folder 
        '''
               
        # Define custom schema of csv files
        schema = StructType([StructField('external_author_id', StringType(), True), 
                             StructField('author', StringType(), True),
                             StructField('content', StringType(), True),
                             StructField('region', StringType(), True),
                             StructField('language', StringType(), True),
                             StructField('publish_date', StringType(), True),
                             StructField('harvested_date', StringType(), True),
                             StructField('following', IntegerType(), True),
                             StructField('followers', IntegerType(), True),
                             StructField('updates', IntegerType(), True),
                             StructField('post_type', StringType(), True),
                             StructField('account_type', StringType(), True),
                             StructField('retweet', IntegerType(), True),
                             StructField('account_category', StringType(), True),
                             StructField('new_june_2018', IntegerType(), True),
                             StructField('alt_external_id', StringType(), True),
                             StructField('tweet_id', StringType(), True),
                             StructField('article_url', StringType(), True),
                             StructField('tco1_step1', StringType(), True),
                             StructField('tco2_step1', StringType(), True),
                             StructField('tco3_step1', StringType(), True)                    
                            ])

        # Create df by loading in all csv files in data_directory with schema
        _df = self.spark.read.format("csv") \
            .option("header", "true") \
            .option("sep",",") \
            .schema(schema) \
            .load(f'{self.data_path}/{folder_name}')
        return _df
    

                
        

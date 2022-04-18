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
        self.install_custom_packages(username)
    
    def install_custom_packages(self, username='mhk9c'):
        # Simple pattern to Install custom packages from Juypter.
        # username = 'mhk9c'
        # Install a pip package in the current Jupyter kernel               
        x=1
        print(f'installing package {x}')
        subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'spark-nlp'])
        
        x += 1
        print(f'installing package {x}')
        subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'plotly'])
        
        x += 1
        print(f'installing package {x}')
        subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'jupyterlab'])
        
        x += 1
        print(f'installing package {x}')
        subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'ipywidgets'])
        
        x += 1
        print(f'installing package {x}')
        subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'jupyter-dash'])
        
        x += 1
        print(f'installing package {x}')
        subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'wordcloud'])
        
        x += 1
        print(f'installing package {x}')
        subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'kaleido'])      
        
        
        
        
        print('Done Installing packages')

        
        
        
        
    
        sys.path.append(f'/home/{username}/.local/lib/python3.8/site-packages/')
        
        
        
    def save_df(self, _spark, _df, folder_name):
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

    
    def load_data(self, _spark, folder_name): 
        '''
        Loads a parquet from a folder.
        '''
        full_path = f'{self.data_path}/{folder_name}'
        _df = _spark.read.parquet(full_path)        
        print(f'Done loading from {full_path}.')
        return _df
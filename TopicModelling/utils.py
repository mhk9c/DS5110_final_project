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
        self.install_custom_packages(username)
    
    def install_custom_packages(self, username='mhk9c'):
        # Simple pattern to Install custom packages from Juypter.
        # username = 'mhk9c'
        # Install a pip package in the current Jupyter kernel        
        subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'spark-nlp'])
        
        
#         sys.executable -m pip install demoji
#         sys.executable -m pip install tldextract

        sys.path.append(f'/home/{username}/.local/lib/python3.8/site-packages/')
        
        
        
    
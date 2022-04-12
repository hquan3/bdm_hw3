import csv
import json
import numpy as np
import pandas as pd

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
sc = pyspark.SparkContext.getOrCreate()
spark = SparkSession(sc)

import sys
from io import StringIO

def returnName(id):
  for i in range(len(sample_items)):
    if id == sample_items['code'][i]:
      return sample_items['Item Name'][i]

def extractProducts(partId, part):
    if partId==0:
        next(part)
    import csv
    for record in csv.reader(part):
      if record[0] in nyc_stores:
        insecurity = int(float(nyc_stores[record[0]]['foodInsecurity'])*100)
        if not(record[2] == 'N/A'):
          id = record[2].split('-')[1]
          name = returnName(id)
          price = float(record[5].split(u'\xa0')[0][1:])
          if name is not None:
            yield (name, price, insecurity)

if __name__ == "__main__":
    products = sc.textFile("/tmp/bdm/keyfood_products.csv", use_unicode=True)

    f = open('keyfood_nyc_stores.json')
    nyc_stores = json.load(f)

    sample_items = pd.read_csv('keyfood_sample_items.csv') 
    sample_items[['key', 'code']] = sample_items['UPC code'].str.split('-', expand=True)
    
    outputTask1 = products.mapPartitionsWithIndex(extractProducts)
    outputTask1.cache()
    outputTask1.write.csv(sys.argv[1])

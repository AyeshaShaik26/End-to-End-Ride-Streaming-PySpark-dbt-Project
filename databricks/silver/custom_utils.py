from typing import List
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import *

class transformationss:
    def dedup(self,df:DataFrame,dedup_cols:List, cdc:str):

        df = df.withColumn("dedup_key", concat(*dedup_cols))
        df = df.withColumn('dedup_counts', row_number()\
                            .over(Window.partitionBy('dedup_key').orderBy(desc(cdc))))
        df =df.filter(col('dedup_counts')==1)
        df = df.drop("dedup_key", 'dedup_counts')

        return df

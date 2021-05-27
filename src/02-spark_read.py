from pathlib import Path

import cn2an
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

root = Path(__file__).parents[1]
file_list = ['A_lbr_land_A.csv', 'B_lbr_land_A.csv', 'E_lbr_land_A.csv', 'F_lbr_land_A.csv', 'H_lbr_land_A.csv']

spark = SparkSession.builder.master('local').getOrCreate()


@udf(returnType=IntegerType())
def floor_transform(x):
    if x is not None:
        res = cn2an.transform(x)
        return int(res[:-1])
    return 0


df = spark.read.csv('../data/A_lvr_land_A.csv', header=True)
df = df.filter(df['主要用途'] == '住家用'). \
    filter(df['建物型態'].substr(1, 4) == '住宅大樓'). \
    filter(floor_transform(df['總樓層數']) >= 13)

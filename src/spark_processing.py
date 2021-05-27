import os
from functools import reduce
from pathlib import Path

import cn2an
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType


# question 2
def load_dataset(spark_session):
    root = Path(__file__).parents[1]
    file_list = ['A_lvr_land_A.csv', 'B_lvr_land_A.csv', 'E_lvr_land_A.csv', 'F_lvr_land_A.csv', 'H_lvr_land_A.csv']
    return [spark_session.read.csv(os.path.join(root, 'data', file), header=True) for file in file_list]


# question 3
def merge_dataset(datasets):
    return reduce(DataFrame.unionAll, datasets)


def filter_dataset(df):
    @udf(returnType=IntegerType())
    def floor_transform(x):
        if x is not None:
            res = cn2an.transform(x)
            return int(res[:-1])
        return 0

    df = df.filter(df['主要用途'] == '住家用'). \
        filter(df['建物型態'].substr(1, 4) == '住宅大樓'). \
        filter(floor_transform(df['總樓層數']) >= 13)
    return df


if __name__ == '__main__':
    spark = SparkSession.builder.master('local').getOrCreate()
    dfs = load_dataset(spark)
    df = merge_dataset(dfs)

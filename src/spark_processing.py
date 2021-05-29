import os
from functools import reduce
from pathlib import Path

import cn2an
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import udf, collect_list, struct
from pyspark.sql.types import StringType, IntegerType


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


# question 4
@udf(returnType=StringType())
def rc2ad(x):
    date = [str(int(x[:3]) + 1911), x[3:5], x[5:]]
    return '-'.join(date)


def output_json(df):
    df = df.withColumn('city', df['土地區段位置建物區段門牌'].substr(1, 3)) \
        .withColumn('date', rc2ad(df['交易年月日'])) \
        .withColumnRenamed('鄉鎮市區', 'district') \
        .withColumnRenamed('建物型態', 'building_state') \
        .select('city', 'date', 'district', 'building_state')
    df = df.sort(df.city.asc(), df.date.desc())
    df = df.withColumn('events', struct(df.district, df.building_state))
    df = df.groupby('city', 'date').agg(collect_list(df.events)) \
        .withColumnRenamed('collect_list(events)', 'events')
    df = df.withColumn('time_slots', struct(df.date, df.events)) \
        .select('city', 'time_slots') \
        .groupby('city') \
        .agg(collect_list('time_slots')) \
        .withColumnRenamed('collect_list(time_slots)', 'time_slots')
    return df


if __name__ == '__main__':
    spark = SparkSession.builder.getOrCreate()
    dfs = load_dataset(spark)
    df = merge_dataset(dfs)
    df = filter_dataset(df)
    df = output_json(df)

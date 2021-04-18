"""
Load data from database and json for reconciliation
"""
import configparser
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, to_date, when, udf
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType


def load_data_from_db(spark_ses, connection, table_name):
    """
    Load data from PostgreSQL Database
    :param spark_ses: spark session
    :param connection: connection properties for PostgreSQL database from config
    :param table_name: postgresql table name
    :return: dataframe with data from table
    """
    data_frame = spark_ses \
        .read \
        .format('jdbc') \
        .option('url', 'jdbc:postgresql://{0}:{1}/{2}'.format(connection['host'], connection['port']
                                                              , connection['database'])) \
        .option('dbtable', table_name) \
        .option('user', connection['user']) \
        .option('password', connection['password']) \
        .option('driver', "org.postgresql.Driver") \
        .load()
    return data_frame


def load_data_from_hdfs(spark_ses, hdfs_con):
    """
    Load json data from hdfs
    :param spark_ses: spark session
    :param hdfs_con: connection properties for hdfs from config
    :return: dataframe with data from json file
    """
    schema_json = StructType([StructField('transaction_uid', StringType(), nullable=False)
                                 , StructField('client_account', StringType(), nullable=True)
                                 , StructField('transaction_date', StringType(), nullable=True)
                                 , StructField('transaction_type', StringType(), nullable=True)
                                 , StructField('amount', DoubleType(), nullable=True)
                              ])
    data_frame = spark_ses.read.json('hdfs://{0}:{1}{2}'.format(hdfs_con['host']
                                                                , hdfs_con['port']
                                                                , hdfs_con['json_path'])
                                     , schema=schema_json)
    return data_frame


def make_column_with_alias(alias, column):
    """
    Add alias for columns
    :param alias: alias of dataframe
    :param column: column in dataframe
    :return: alias for column
    """
    return col('{0}.{1}'.format(alias, column)).alias('{0}_{1}'.format(alias, column))


def reconciliation(df1, df2, primary_key):
    """
    Doing reconciliation between 2 datasets
    :param primary_key: field which using for joining
    :param df1: dataframe based on source 1
    :param df2: dataframe based on source 2
    :return: total_dataframe with comparing information
    """
    common_df = df1.alias('df1') \
        .join(df2.alias('df2')
              , col('df1.{0}'.format(primary_key)) == col('df2.{0}'.format(primary_key))
              , 'full') \
        .select([make_column_with_alias('df1', column) for column in df1.columns]
                + [make_column_with_alias('df2', column) for column in df2.columns])
    for column in df1.schema.names:
        if column == 'transaction_uid':
            pass
        elif column == 'amount':
            check_tolerance_udf = udf(check_tolerance_interval, IntegerType())
            common_df = common_df \
                .withColumn('{0}_is_equal'.format(column)
                            , check_tolerance_udf(col('df1_{0}'.format(column))
                                                  , col('df2_{0}'.format(column))))
        else:
            common_df = common_df \
                .withColumn('{0}_is_equal'.format(column)
                            , when(col('df1_{0}'.format(column)) == col('df2_{0}'.format(column))
                                   , 1).otherwise(0))
    return common_df


def check_tolerance_interval(first_num, second_num):
    """
    Check equaling of two numbers with tolerance interval
    :param second_num: The first comparing value
    :param first_num: The second comparing value
    :return: 1 if value in tolerance interval. 0 if not
    """
    if first_num == second_num:
        return 1

    tolerance = 0
    try:
        tolerance = float(sys.argv[1])
        if tolerance > 100 or tolerance < 0:
            print('Parameter should be between 0 and 100. Using constant value 0')
    except ValueError:
        print('Parameter should be numeric. Using constant value 0')
    except IndexError:
        print('Parameter doesnt exist. Using constant value 0')

    tolerance_value_first = first_num * tolerance / 100
    tolerance_value_second = second_num * tolerance / 100
    if second_num - tolerance_value_second <= first_num <= second_num + tolerance_value_second:
        return 1
    elif first_num - tolerance_value_first <= second_num <= first_num + tolerance_value_first:
        return 1
    else:
        return 0


if __name__ == "__main__":

    spark = SparkSession \
        .builder \
        .appName('load_raw_level') \
        .master('spark://spark-master:7077') \
        .getOrCreate()

    config = configparser.ConfigParser()
    config.read('/opt/spark_jobs/config.ini')

    df_pg = load_data_from_db(spark, config['db_connection'], 'transactions')
    df_json = load_data_from_hdfs(spark, config['hdfs_connection'])

    df_pg = df_pg.withColumn('transaction_type', lower(col('transaction_type')))
    df_json = df_json.withColumn('transaction_type', lower(col('transaction_type')))
    df_json = df_json.withColumn('transaction_date', to_date(col('transaction_date')))
    df_json = df_json.withColumn('amount', col('amount').cast('decimal(20,4)'))

    reconciliation(df_pg, df_json, 'transaction_uid')

    spark.stop()

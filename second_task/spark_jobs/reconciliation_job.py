"""
Load data from database and json for reconciliation.
Input - tolerance percent (default 0)
Output - csv files with statistics and corrupted rows
"""
import configparser
import sys
import logging
import decimal
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, to_date, when, udf, lit
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


def reconciliation(df1, df2, tolerance_percent, primary_key):
    """
    Doing reconciliation between 2 datasets
    :param tolerance_percent: tolerance interval. Value between 0 and 100
    :param primary_key: field which using for joining
    :param df1: dataframe based on source 1
    :param df2: dataframe based on source 2
    :return: total_dataframe with comparing information
    """
    # Full Join using uid. Don't replace nulls in uid column. It uses for statistic
    common_df = df1.alias('df1') \
        .join(df2.alias('df2')
              , col('df1.{0}'.format(primary_key)) == col('df2.{0}'.format(primary_key))
              , 'full') \
        .select([make_column_with_alias('df1', column) for column in df1.columns]
                + [make_column_with_alias('df2', column) for column in df2.columns])

    # Replace nulls for comparing
    common_df = common_df.fillna('', subset=[c for c in common_df.columns
                                             if c not in {'df1_transaction_uid'
                                                          , 'df2_transaction_uid'}])
    common_df = common_df.fillna(0)
    cols_for_comparing = []

    for column in df1.schema.names:
        if column == 'transaction_uid':
            continue
        new_col_name = '{0}_is_equal'.format(column)
        cols_for_comparing.append(new_col_name)
        if column == 'amount':
            # For numeric type we can put tolerance percentage. Using udf function for comparing
            check_tolerance_udf = udf(check_tolerance_interval, IntegerType())
            common_df = common_df \
                .withColumn(new_col_name
                            , check_tolerance_udf(col('df1_{0}'.format(column))
                                                  , col('df2_{0}'.format(column))
                                                  , lit(tolerance_percent)))
        else:
            common_df = common_df \
                .withColumn(new_col_name
                            , when(col('df1_{0}'.format(column)) == col('df2_{0}'.format(column))
                                   , 1).otherwise(0))
    return common_df


def check_tolerance_interval(first_num, second_num, tolerance_percent):
    """
    Check equaling of two numbers with tolerance interval
    :param tolerance_percent: tolerance interval. Value between 0 and 100
    :param second_num: The first comparing value
    :param first_num: The second comparing value
    :return: 1 if value in tolerance interval. 0 if not
    """
    if first_num == second_num:
        return 1

    tolerance_value_first = first_num * tolerance_percent / 100
    tolerance_value_second = second_num * tolerance_percent / 100
    if second_num - tolerance_value_second <= first_num <= second_num + tolerance_value_second:
        return 1
    if first_num - tolerance_value_first <= second_num <= first_num + tolerance_value_first:
        return 1
    return 0


def get_total_statistic(spark_ses, hdfs_con, df_inp, tolerance_percent):
    """
    Make report with information about match/mismatch data
    :param tolerance_percent: tolerance interval. Value between 0 and 100
    :param spark_ses:
    :param df_inp:
    :param hdfs_con:
    :return:
    """
    columns_of_df_stat = ['field_name', 'matching_record_count'
                          , 'mismatch_record_count', 'matching_record_percentage']
    # Count matching rows
    c_matching = df_inp.filter(col('df1_transaction_uid').isNotNull()
                               & col('df2_transaction_uid').isNotNull()).count()
    # Count mismatch rows in db dataset
    c_mismatch_db = df_inp.filter(col('df1_transaction_uid').isNotNull()
                                  & col('df2_transaction_uid').isNull()).count()
    # Count mismatch rows in json dataset
    c_mismatch_json = df_inp.filter(col('df1_transaction_uid').isNull()
                                    & col('df2_transaction_uid').isNotNull()).count()
    rows_of_df_stat = [('matching_record_count', c_matching, 0, 0.0)
                       , ('mismatch_records_from_db_count', c_mismatch_db, 0, 0.0)
                       , ('mismatch_records_from_json_count', c_mismatch_json, 0, 0.0)]
    # Count match / mismatch rows on other fields
    for column in df_inp.columns:
        if column.endswith('is_equal'):
            match_count = df_inp.filter(col(column) == 1).count()
            mismatch_count = df_inp.filter(col(column) == 0).count()
            rows_of_df_stat.append((column[:-9], match_count, mismatch_count
                                    , float((1 - mismatch_count / match_count) * 100)))
    rows_of_df_stat.append(('Tolerance percent is {0} %'.format(tolerance_percent), 0, 0, 0.0))

    df_statistic = spark_ses.createDataFrame(rows_of_df_stat, columns_of_df_stat)
    df_statistic\
        .coalesce(1)\
        .write\
        .mode('overwrite')\
        .csv('hdfs://{0}:{1}/reports/report.csv'.format(hdfs_con['host']
                                                        , hdfs_con['port'])
             , header='true')


def write_corrupted_rows(hdfs_con, df_inp):
    """
    Load mismatch rows into hdfs
    :param hdfs_con: hdfs connection info
    :param df_inp: dataframe with corrupted rows
    :return:
    """
    hdfs_url = 'hdfs://{0}:{1}/'.format(hdfs_con['host']
                                        , hdfs_con['port'])
    df_inp\
        .filter(col('df1_transaction_uid').isNotNull()
                & col('df2_transaction_uid').isNull())\
        .coalesce(1)\
        .write\
        .mode('overwrite')\
        .csv('{0}errors/db_rows.csv'.format(hdfs_url))
    df_inp\
        .filter(col('df1_transaction_uid').isNull()
                & col('df2_transaction_uid').isNotNull())\
        .coalesce(1)\
        .write\
        .mode('overwrite')\
        .csv('{0}errors/json_rows.csv'.format(hdfs_url))

    for column in df_inp.columns:
        if column.endswith('is_equal'):
            df_inp\
                .filter(col(column) == 0)\
                .coalesce(1)\
                .write\
                .mode('overwrite')\
                .csv('{0}errors/{1}_mismatch.csv'.format(hdfs_url, column[:-9]))


if __name__ == "__main__":

    tolerance = 0
    try:
        tolerance = decimal.Decimal(sys.argv[1])
        if tolerance > 100 or tolerance < 0:
            logging.warning('Parameter should be between 0 and 100. Using constant value 0')
    except ValueError:
        logging.warning('Parameter should be numeric. Using constant value 0')
    except IndexError:
        logging.warning('Parameter doesnt exist. Using constant value 0')

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

    df_total = reconciliation(df_pg, df_json, tolerance, 'transaction_uid')
    get_total_statistic(spark, config['hdfs_connection'], df_total, tolerance)
    write_corrupted_rows(config['hdfs_connection'], df_total)

    spark.stop()

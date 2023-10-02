import sys

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from lib.logger import Log4J
from lib.utils import (
    get_spark_app_config, 
    get_postgres_config,
    load_table,
    write_table
)
from pyspark.sql.functions import lit
from pyspark.sql.types import StructType, StructField, DateType, StringType, IntegerType
from lib.queries import (
    highest_score_per_event_query,
    top_10_most_popular_exams_query,
    top_3_most_popular_exams_each_place_query,
    create_top_3_most_popular_exams_each_place_query
)



def get_conf_file():
    conf_file = None

    if '--conf_file' in sys.argv:
        conf_file_index = sys.argv.index('--conf_file') + 1
        conf_file = sys.argv[conf_file_index]
    else:
        print("Conf file argument not provided.")

    return conf_file


def get_load_date(logger):
    load_date = None
    if '--date' in sys.argv:
        date_index = sys.argv.index('--date') + 1
        load_date = sys.argv[date_index]
        logger.info("Today's Date: " + load_date)
    else:
        logger.error("Date argument not provided.")

    return load_date


if __name__ == '__main__':

    conf_file = get_conf_file()
    conf = get_spark_app_config(conf_file)

    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()
    
    logger = Log4J(spark)
    logger.info('Spark started!')
    
    pg_config = get_postgres_config(conf_file)
    db = 'education'

    load_date = get_load_date(logger)
    logger.info('Loading attempts for: ' + load_date)

    table = 'attempts'
    attempts_df = load_table(spark, db, table, pg_config)
    attempts_df.createOrReplaceTempView('attempts_df_tbl')

    table = 'students'
    students_df = load_table(spark, db, table, pg_config)
    students_df.createOrReplaceTempView('students_df_tbl')

    table = 'questions'
    questions_df = load_table(spark, db, table, pg_config)
    questions_df.createOrReplaceTempView('questions_df_tbl')
    
    table = 'exams'
    exams_df = load_table(spark, db, table, pg_config)
    exams_df.createOrReplaceTempView('exams_df_tbl')

    # ANALYTICS
    highest_score_per_event_df = spark.sql(highest_score_per_event_query)
    highest_score_per_event_df = highest_score_per_event_df \
        .withColumn('ATTEMPT_DT', lit(load_date).cast(DateType()))

    table = 'highest_score_per_event'
    mode = 'append'
    write_table(highest_score_per_event_df, db, table, pg_config, mode)

    top_10_most_popular_exams_df = spark.sql(top_10_most_popular_exams_query)
    top_10_most_popular_exams_df = top_10_most_popular_exams_df \
        .withColumn('ATTEMPT_DT', lit(load_date).cast(DateType()))

    table = 'top_10_most_popular_exams'
    mode = 'append'
    write_table(top_10_most_popular_exams_df, db, table, pg_config, mode)

    top_3_most_popular_exams_each_place_df = spark.sql(top_3_most_popular_exams_each_place_query)
    top_3_most_popular_exams_each_place_df = top_3_most_popular_exams_each_place_df \
        .withColumn('ATTEMPT_DT', lit(load_date).cast(DateType()))
    
    table = 'top_3_most_popular_exams_each_place'
    mode = 'append'
    write_table(top_3_most_popular_exams_each_place_df, db, table, pg_config, mode)

    spark.stop()

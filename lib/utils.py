import configparser
from pyspark import SparkConf


def get_spark_app_config(conf_file):
    conf = SparkConf()

    config = configparser.ConfigParser()
    config.read(conf_file)
    
    for (key, val) in config.items('SPARK_APP_CONFIGS'):
        conf.set(key, val)

    return conf


def get_postgres_config(conf_file):
    conf = {}
    config = configparser.ConfigParser()
    config.read(conf_file)
    
    for (key, val) in config.items('POSTGRES_CONFIGS'):
        conf[key] = val

    return conf


def load_table(spark, db, table, pg_config):
    pg_config['url'] = f'jdbc:postgresql://{pg_config["server.ip"]}:{pg_config["server.port"]}/{db}'
    pg_config['table'] = table

    return spark.read \
        .format('jdbc') \
        .option('url', pg_config['url']) \
        .option('driver', pg_config['driver']) \
        .option('user', pg_config['user']) \
        .option('password', pg_config['password']) \
        .option('dbtable', pg_config['table']) \
        .load()


def write_table(df, db, table, pg_config, mode):
    pg_config['url'] = f'jdbc:postgresql://{pg_config["server.ip"]}:{pg_config["server.port"]}/{db}'
    pg_config['table'] = table

    return df.write \
        .format('jdbc') \
        .option('url', pg_config['url']) \
        .option('driver', pg_config['driver']) \
        .option('user', pg_config['user']) \
        .option('password', pg_config['password']) \
        .option('dbtable', pg_config['table']) \
        .mode(mode) \
        .save()

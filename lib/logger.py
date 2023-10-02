class Log4J:
    def __init__(self, spark):
        log4j = spark._jvm.org.apache.log4j
        root_class = 'myorg'
        conf = spark.sparkContext.getConf()

        self.logger = log4j.LogManager.getLogger(
            root_class + '.' + conf.get('spark.app.name')
        )

    def warn(self, message):
        self.logger.warn(message)
    
    def info(self, message):
        self.logger.info(message)
    
    def error(self, message):
        self.logger.error(message)
    
    def debug(self, message):
        self.logger.debug(message)

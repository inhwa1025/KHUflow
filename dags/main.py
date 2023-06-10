from pyspark import SparkContext
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

def load_config(spark_context: SparkContext):
    spark_context._jsc.hadoopConfiguration().set('fs.s3a.access.key', 'myaccesskey')
    spark_context._jsc.hadoopConfiguration().set('fs.s3a.secret.key', 'mysecretkey')
    spark_context._jsc.hadoopConfiguration().set('fs.s3a.path.style.access', 'true')
    spark_context._jsc.hadoopConfiguration().set('fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
    spark_context._jsc.hadoopConfiguration().set('fs.s3a.endpoint', 'http://133.186.150.141:9000') // minio의 외부에 공개된 port 설정
    spark_context._jsc.hadoopConfiguration().set('fs.s3a.connection.ssl.enabled', 'false')

load_config(spark.sparkContext)

dataframe = spark.read.json('s3a://test/*') //orders.json 파일 선택

average = dataframe.agg({'amount': 'avg'}) //orderjs.json 파일의 amount의 평균값

average.show()

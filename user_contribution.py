import sys
assert sys.version_info >= (3, 5) 

from pyspark.sql import SparkSession, functions, types as t, DataFrameReader

def main(inputs, output):
    q_schema= t.StructType([
        t.StructField('id', t.StringType()),
        t.StructField('owner_id', t.StringType()),
        t.StructField('open_timestamp', t.TimestampType()),
        t.StructField('close_timestamp', t.TimestampType()),
        t.StructField('score', t.LongType()),
        t.StructField('title', t.StringType()),
        t.StructField('body', t.StringType())
    ])

    qs = spark.read.csv(inputs,
                        schema=q_schema,
                        header=True,
                        samplingRatio=0.001,
                        multiLine=True,
                        escape='"',)
    
    qs.show()
    print(qs.schema)

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('Final Project: User Contribution').getOrCreate()
    assert spark.version >= '3.0' 
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)
import sys
assert sys.version_info >= (3, 5) 

from pyspark.sql import SparkSession, functions as f, types as t

def read_q_and_a_data(input_q, input_a):
    q_schema= t.StructType([
        t.StructField('id', t.StringType()),
        t.StructField('owner_id', t.StringType()),
        t.StructField('creation_timestamp', t.TimestampType()),
        t.StructField('close_timestamp', t.TimestampType()),
        t.StructField('score', t.LongType()),
        t.StructField('title', t.StringType()),
        t.StructField('body', t.StringType())
    ])

    q_df = spark.read.csv(input_q,
                        schema=q_schema,
                        header=True,
                        multiLine=True,
                        escape='"',)
    
    a_schema = t.StructType([
        t.StructField('id', t.StringType()),
        t.StructField('owner_id', t.StringType()),
        t.StructField('creation_timestamp', t.TimestampType()),
        t.StructField('parent_id', t.StringType()),
        t.StructField('score', t.LongType()),
        t.StructField('body', t.StringType())
    ])

    a_df = spark.read.csv(input_a,
                      schema=a_schema,
                        header=True,
                        multiLine=True,
                        escape='"',)
    
    return q_df, a_df

def main(input_q, input_a, output):
    q_df, a_df = read_q_and_a_data(input_q, input_a)
    q_counts_by_user = q_df.groupBy('owner_id').count().withColumnRenamed('count', 'n_q')
    a_counts_by_user = a_df.groupBy('owner_id').count().withColumnRenamed('count', 'n_a')
    qa_counts_by_user = q_counts_by_user.join(a_counts_by_user, 'owner_id')
    qa_counts_by_user.cache()
    q_ranking = qa_counts_by_user.orderBy(f.desc('n_q'))
    a_ranking = qa_counts_by_user.orderBy(f.desc('n_q'))
    q_ranking.show()
    a_ranking.show()


if __name__ == '__main__':
    input_q = sys.argv[1]
    input_a = sys.argv[2]
    output = sys.argv[3]
    spark = SparkSession.builder.appName('Final Project: User Contribution').getOrCreate()
    assert spark.version >= '3.0' 
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(input_q, input_a, output)
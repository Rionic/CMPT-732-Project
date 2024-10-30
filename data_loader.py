from pyspark.sql import types as t


def read_data(spark, input_q, input_a, input_t):
    '''
    Reads questions, answers, and tags csvs from memory as a DataFrame.

    Args:
        input_q (str): Directory containing question csv(s).
        input_a (str): Directory containing answer csv(s).
        input_t (str): Directory containing tag csv(s).

    Returns:
        (DataFrame, DataFrame, DataFrame): A question, answer, and tag DataFrames, respectively. 
    '''
    if not input_q:
        raise ValueError("Questions path is empty")

    if not input_a:
        raise ValueError("Answers path is empty")

    if not input_t:
        raise ValueError("Tags path is empty")

    q_schema = t.StructType([
        t.StructField('Id', t.StringType()),
        t.StructField('OwnerUserId', t.StringType()),
        t.StructField('CreationDate', t.TimestampType()),
        t.StructField('ClosedDate', t.TimestampType()),
        t.StructField('Score', t.LongType()),
        t.StructField('Title', t.StringType()),
        t.StructField('Body', t.StringType())
    ])

    q_df = spark.read.csv(input_q,
                          schema=q_schema,
                          header=True,
                          multiLine=True,
                          escape='"',)

    a_schema = t.StructType([
        t.StructField('Id', t.StringType()),
        t.StructField('OwnerUserId', t.StringType()),
        t.StructField('CreationDate', t.TimestampType()),
        t.StructField('ParentId', t.StringType()),
        t.StructField('Score', t.LongType()),
        t.StructField('Body', t.StringType())
    ])

    a_df = spark.read.csv(input_a,
                          schema=a_schema,
                          header=True,
                          multiLine=True,
                          escape='"',)

    t_schema = t.StructType([
        t.StructField('Id', t.StringType()),
        t.StructField('Tag', t.StringType())
    ])

    t_df = spark.read.csv(input_t,
                          schema=t_schema,
                          header=True,
                          multiLine=True,
                          escape='"')

    return q_df, a_df, t_df

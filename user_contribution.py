'''
Calculates a percentile breakdown of the top percentiles users (in intervals of n %) vs the fraction of questions and answers they produce.
Command line args are question directory, answer directory, output directory and n.
'''
import sys
assert sys.version_info >= (3, 5)

from pyspark.sql import SparkSession, functions as f, types as t, Window

def read_q_and_a_data(input_q, input_a):
    '''
    Reads questions and answers csvs from memory as a DataFrame.

    Args:
        input_q (str): Directory containing question csv(s).
        input_a (str): Directory containing answer csv(s).

    Returns:
        (DataFrame, DataFrame): A question and answer DataFrames, respectively. 
    '''
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

def get_cumsum(df, field, granularity=None):
    '''
    Given a DataFrame which relates owners to a sum of entities, returns a percentile breakdown of the fraction of entities. Ignores rows where owner_id is 'NA'.

    Args:
        df (DataFrame): DataFrame containing a row called 'owner_id' and a row that corresponds to the field arg.
        arg (str): Column name of the entity whose fractions will be determined by percentile.
        granularity (int): The percentage interval between rows in the returned DataFrame. E.g) 5 means create a row for every 5% multiple percentile.
    
    Returns:
        DataFrame containing percentiles of owners and corresponding fraction of field.
    '''
    total_field = df.groupBy().sum(field).head()[0]
    df = df.where(df['owner_id'] != 'NA')
    total_owners = df.count()
    wdw = Window.orderBy(f.col(field).desc()).rowsBetween(Window.unboundedPreceding, Window.currentRow)
    df = df.select(
        (f.sum(field).over(wdw) / total_field).alias(f'{field}_fraction'),
        (f.row_number().over(wdw) / total_owners).alias('percentile'),
        f.row_number().over(wdw).alias('row')
        )

    if granularity:      
        breakpoints = {int (total_owners * i / (100 / granularity)) for i in range(1, 100)}
        df = df.where(f.col('row').isin(breakpoints))

    df = df.drop('row')
    return df

def main(input_q, input_a, output, gran):
    q_df, a_df = read_q_and_a_data(input_q, input_a)

    q_counts_by_user = q_df.groupBy('owner_id').count().withColumnRenamed('count', 'n_q')
    q_sums = q_counts_by_user.groupBy('owner_id').agg(f.sum('n_q').alias('n_q'))
    q_ranking = q_sums.orderBy(f.desc('n_q'))
    q_ranking.cache()
    q_cumsums = get_cumsum(q_ranking, 'n_q', gran)
    q_cumsums.write.mode('overwrite').parquet(f'{output}/question_user_percentiles')

    a_counts_by_user = a_df.groupBy('owner_id').count().withColumnRenamed('count', 'n_a')
    a_sums = a_counts_by_user.groupBy('owner_id').agg(f.sum('n_a').alias('n_a'))
    a_ranking = a_sums.orderBy(f.desc('n_a'))
    a_ranking.cache()
    a_cumsums = get_cumsum(a_ranking, 'n_a', gran)
    a_cumsums.write.mode('overwrite').parquet(f'{output}/answer_user_percentiles')

    # qa_counts_by_user = q_counts_by_user.join(a_counts_by_user, 'owner_id')
    # print(qa_counts_by_user.corr('n_q', 'n_a'))

if __name__ == '__main__':
    input_q = sys.argv[1]
    input_a = sys.argv[2]
    output = sys.argv[3]
    gran = int(sys.argv[4])
    spark = SparkSession.builder.appName('Final Project: User Contribution').getOrCreate()
    assert spark.version >= '3.0'
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(input_q, input_a, output, gran)
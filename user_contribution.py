'''
Calculates a percentile breakdown of the top percentiles users vs the fraction of questions and answers they produce.
Command line args are question directory, answer directory and output directory.
'''
from pyspark.sql import SparkSession, functions as f, types as t, Window
import sys
assert sys.version_info >= (3, 5)


def get_percentiles(df, field):
    '''
    Calculates an approx. breakdown of percentile of users vs the fraction of 'field' that they are responsible for.

    Arguments:
        df (DataFrame): DataFrame containing user and field data.
        field (str): Name of the field that is being mapped against percentile (e.g. answers, questions).

    Returns:
        A DataFrame containing the percentiles (where defined), fraction of 'field' and number of field per user that near that percentile. 
    '''
    field_counts_by_user_with_NA = df.groupBy(
        'OwnerUserId').agg(f.count('OwnerUserID').alias('count'))
    field_counts_by_user_with_NA.cache()
    total_field = field_counts_by_user_with_NA.groupBy().sum('count').first()[
        'sum(count)']
    field_counts_by_user = field_counts_by_user_with_NA.where(
        f.col('OwnerUserId') != 'NA')
    field_counts_by_user_sum = field_counts_by_user.groupBy(
        'count').agg(f.sum('count').alias('total'))
    field_counts_by_user_sum.cache()

    # While this Window will coalesce the data to 1 partition, this is okay because there
    # will only be as many rows as the maximum number of field occurences that a single user made.
    # e.g.) If the user who asked the most questions had asked a total of 10000,
    # the DataFrame would only contain between [1, 10000] rows.
    wdw = Window().orderBy(f.desc('count')).rowsBetween(
        Window.unboundedPreceding, Window.currentRow)
    field_fracs = field_counts_by_user_sum.select(
        f.col('count'),
        (f.sum('total').over(wdw) / total_field).alias('total')
    )

    interval = 10000
    percentiles = list(map(lambda i: i / interval, range(1, interval)))
    percentile_breakpoints = field_counts_by_user.select(
        f.percentile_approx('count', percentiles)).head()[0]
    percentile_labels = map(lambda i: i / (interval / 100), range(1, interval))
    percentile_scores = list(zip(percentile_labels, percentile_breakpoints))
    perc_df = spark.createDataFrame(
        percentile_scores, schema=['percentile', 'score'])
    perc_df = perc_df.groupBy('score').agg(
        f.max('percentile').alias('percentile'))
    res_df = perc_df.join(
        field_fracs, [perc_df['score'] == field_fracs['count']])
    res_df = res_df.select(
        f.col('percentile'),
        f.col('total').alias(f'fraction_of_{field}_created'),
        f.col('count')
    ).orderBy(f.desc('percentile'))

    return res_df


def main(input_q, input_a,  output):
    q_df = spark.read.parquet(input_q)
    a_df = spark.read.parquet(input_a)
    q_perc = get_percentiles(q_df, 'questions')
    q_perc.show()
    q_perc.write.mode('overwrite').parquet(
        f'{output}/question_user_percentiles')
    a_perc = get_percentiles(a_df, 'answers')
    a_perc.show()
    a_perc.write.mode('overwrite').parquet(f'{output}/answer_user_percentiles')


if __name__ == '__main__':
    input_q = sys.argv[1]
    input_a = sys.argv[2]
    output = sys.argv[3]
    spark = SparkSession.builder.appName(
        'Final Project: User Contribution').getOrCreate()
    assert spark.version >= '3.0'
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(input_q, input_a, output)

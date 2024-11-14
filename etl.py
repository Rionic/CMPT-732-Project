from pyspark.sql import SparkSession, functions as F, types as t
import sys
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+


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


def find_top_tags(t_df):
    # Find the top 10 tags by counting occurrences in tags DataFrame
    top_tags_df = (
        t_df.groupBy('Tag')
        .count()
        .orderBy(F.desc('count'))
        .limit(10)
    )
    return [row['Tag'] for row in top_tags_df.collect()]


def join_tags_with_questions(q_df, t_df, top_tags):
    # Filter t_df to include only rows with tags in the top 10
    filtered_t_df = t_df.filter(t_df.Tag.isin(top_tags))

    # Join questions with the filtered tags DataFrame
    joined_df = q_df.join(filtered_t_df, q_df.Id ==
                          filtered_t_df.Id, 'left').drop(filtered_t_df.Id)
    return joined_df


def join_tags_with_answers(a_df, t_df, top_tags):
    # Filter t_df to include only rows with tags in the top 10
    filtered_t_df = t_df.filter(t_df.Tag.isin(top_tags))

    # Join questions with the filtered tags DataFrame
    joined_df = a_df.join(filtered_t_df, a_df.ParentId ==
                          filtered_t_df.Id, 'left').drop(filtered_t_df.Id)
    return joined_df


def drop_early_answers(a_df, q_df):

    # Rename and select relevant columns for the incoming join
    q_df = q_df.withColumnRenamed('Id', 'QuestionId'). \
        withColumnRenamed('CreationDate', 'QuestionDate'). \
        select('QuestionId', 'QuestionDate')

    # Join t_q df with answers df to get t_q_a df
    q_a_df = q_df.join(a_df, q_df['QuestionId'] == a_df['ParentId']). \
        withColumnRenamed('CreationDate', 'AnswerDate')

    # Drop rows with an answer posted before the question and select the Id
    valid_dates = q_a_df.filter('QuestionDate < AnswerDate').select('Id')

    # Join valid_dates to a_df so the invalid dates are dropped in a_df
    a_df = a_df.join(valid_dates, 'Id').dropDuplicates()

    return a_df


def main(input_path_questions, input_path_answers, input_path_tags, output):
    q_df, a_df, t_df = read_data(
        spark, input_path_questions, input_path_answers, input_path_tags)

    q_df = q_df.drop('Title', 'Body', 'ClosedDate')
    a_df = a_df.drop('Body')

    # Find top 10 tags in tags DataFrame
    top_tags = find_top_tags(t_df)

    # Join tags with questions using only top tags
    questions_with_tags_df = join_tags_with_questions(q_df, t_df, top_tags)
    questions_with_tags_df = questions_with_tags_df.filter(
        questions_with_tags_df.Tag.isNotNull())

    # Join tags with answers using only top tags
    answers_with_tags_df = join_tags_with_answers(a_df, t_df, top_tags)
    answers_with_tags_df = answers_with_tags_df.filter(
        answers_with_tags_df.Tag.isNotNull())

    # Drop rows where the answer was posted before the question
    answers_with_tags_df = drop_early_answers(
        answers_with_tags_df, questions_with_tags_df)

    # Repartition by tag to make future groupbys more efficient
    questions_with_tags_df = questions_with_tags_df.repartition("Tag")
    answers_with_tags_df = answers_with_tags_df.repartition("Tag")

    # Write DataFrames to Parquet
    questions_with_tags_df.write.mode(
        'overwrite').parquet(f"{output}/questions")
    answers_with_tags_df.write.mode('overwrite').parquet(
        f"{output}/answers")


if __name__ == '__main__':
    input_path_questions = sys.argv[1]
    input_path_answers = sys.argv[2]
    input_path_tags = sys.argv[3]
    output = sys.argv[4]
    spark = SparkSession.builder.appName(
        'Final Project: ETL').getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(input_path_questions, input_path_answers, input_path_tags, output)

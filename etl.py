from data_loader import read_data
#from answer_speed import calc_answer_speed
from pyspark.sql import SparkSession, functions as F
import sys
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

def find_top_tags(t_df):
    # Find the top 10 tags by counting occurrences in tags DataFrame
    top_tags_df = (
        t_df.groupBy('Tag')
        .count()
        .orderBy(F.desc('count'))
        .limit(10)
    )
    top_tags_df.show()
    return [row['Tag'] for row in top_tags_df.collect()]


def join_tags_with_questions(q_df, t_df, top_tags):
    # Filter t_df to include only rows with tags in the top 10
    filtered_t_df = t_df.filter(t_df.Tag.isin(top_tags))
    
    # Join questions with the filtered tags DataFrame
    joined_df = q_df.join(filtered_t_df, q_df.Id == filtered_t_df.Id, 'left').drop(filtered_t_df.Id)
    return joined_df


def main(input_path_questions, input_path_answers, input_path_tags, output):
    q_df, a_df, t_df = read_data(
        spark, input_path_questions, input_path_answers, input_path_tags)

    q_df = q_df.drop('Title', 'Body', 'ClosedDate')
    a_df = a_df.drop('Body')

    # Find top 10 tags in tags DataFrame
    top_tags = find_top_tags(t_df)

    # Join tags with questions using only top tags
    questions_with_tags_df = join_tags_with_questions(q_df, t_df, top_tags)
    questions_with_tags_df = questions_with_tags_df.filter(questions_with_tags_df.Tag.isNotNull())

    # Write DataFrames to Parquet
    questions_with_tags_df.write.parquet(f"{output}/questions")
    a_df.write.parquet(f"{output}/answers")


if __name__ == '__main__':
    input_path_questions = sys.argv[1]
    input_path_answers = sys.argv[2]
    input_path_tags = sys.argv[3]
    output = sys.argv[4]
    spark = SparkSession.builder.appName(
        'Final Project: User Engagement').getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(input_path_questions, input_path_answers, input_path_tags, output)

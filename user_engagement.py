from pyspark.sql import SparkSession, functions as F, types as T
import sys
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+


def user_engagement(input_t_q, input_t_a):
    t_q_df = spark.read.parquet(input_t_q)
    t_a_df = spark.read.parquet(input_t_a)

    # Calculate total and average question score per tag
    question_score_by_tag = t_q_df.groupBy("Tag").agg(F.sum("Score").alias(
        "total_question_score"), F.avg("Score").alias("average_question_score"))

    # Calculate the total number of answers per tag
    answer_count_by_tag = t_a_df.groupBy("Tag").agg(
        F.count("ParentId").alias("total_answers"))

    # Calculate total and average answer score per tag
    answer_score_by_tag = t_a_df.groupBy("Tag").agg(F.sum("Score").alias(
        "total_answer_score"), F.avg("Score").alias("avg_answer_score"))

    # Join the three DataFrames on "Tag" to have a single DataFrame with all metrics
    engagement_by_tag = question_score_by_tag.join(answer_count_by_tag, "Tag", "outer").join(
        answer_score_by_tag, "Tag", "outer").na.fill(0)

    # Display results
    engagement_by_tag.show(100)

    engagement_by_tag.write.mode('overwrite').parquet(
        "output/engagement_by_tag")


if __name__ == '__main__':
    input_path_tag_questions = sys.argv[1]
    input_path_tag_answers = sys.argv[2]
    spark = SparkSession.builder.appName(
        'Final Project: User Engagement').getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    user_engagement(input_path_tag_questions, input_path_tag_answers)

from data_loader import read_data
from pyspark.sql import SparkSession
import sys
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+


# add more functions as necessary


def main(input_path_questions, input_path_answers, input_path_tags, output):
    q_df, a_df, t_df = read_data(
        spark, input_path_questions, input_path_answers, input_path_tags)

    q_df.show(10)
    a_df.show(10)
    t_df.show(10)


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

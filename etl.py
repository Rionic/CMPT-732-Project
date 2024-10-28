from pyspark.sql import SparkSession, functions, types
import sys
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+


# add more functions as necessary


def main(input_path_questions, input_path_answers, input_path_tags, output):
    if not output:
        raise ValueError("Output path is empty")

    print(f"Questions Path: {input_path_questions}")
    print(f"Answers Path: {input_path_answers}")
    print(f"Tags Path: {input_path_tags}")
    print(f"Output Path: {output}")

    # main logic starts here
    questions_df = spark.read.csv(
        input_path_questions, header=True, inferSchema=True)
    answers_df = spark.read.csv(
        input_path_answers, header=True, inferSchema=True)
    tags_df = spark.read.csv(
        input_path_tags, header=True, inferSchema=True)

    questions_subset_df = questions_df.limit(10)
    questions_subset_df.write.mode('overwrite').parquet(output)


if __name__ == '__main__':
    input_path_questions = sys.argv[1]
    input_path_answers = sys.argv[2]
    input_path_tags = sys.argv[3]
    output = sys.argv[4]
    spark = SparkSession.builder.appName('example code').getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(input_path_questions, input_path_answers, input_path_tags, output)

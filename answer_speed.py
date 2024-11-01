from pyspark.sql import SparkSession, functions as F
import sys
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

def calc_answer_speed(input_q, input_a):
    q_df = spark.read.parquet(input_q)
    a_df = spark.read.parquet(input_a)

    # Rename columns and drop score for the incoming join
    q_df = q_df.withColumnRenamed('Id', 'QuestionId'). \
        withColumnRenamed('CreationDate', 'QuestionDate'). \
        drop('Score')
        
    a_df = a_df.drop('Tag')
        
    # Join t_q df with answers df to get t_q_a df
    t_q_a_df = q_df.join(a_df, q_df['QuestionId'] == a_df['ParentId']). \
        select('QuestionId', 'Tag', 'QuestionDate', 'CreationDate', 'Score'). \
        withColumnRenamed('CreationDate', 'AnswerDate')

    # We will not consider responses with score < 3 as an answer
    t_q_a_df = t_q_a_df.filter('Score > 2')

    # Take the date of the first answer
    fastest_answer = t_q_a_df.groupBy('QuestionId').agg({'AnswerDate': 'min'}). \
        withColumnRenamed('min(AnswerDate)', 'AnswerDate')

    # Join the fastest_answer with t_q_a_df to get the answer
    answer = fastest_answer.join(t_q_a_df, ['QuestionId', 'AnswerDate'])

    # Drop duplicates in case of a tie
    answer = answer.dropDuplicates(['QuestionId', 'AnswerDate', 'Tag'])

    # Get the time taken for each answer
    answer_speed = answer.withColumn(
        'AnswerSpeed',  (F.unix_timestamp('AnswerDate') -
                         F.unix_timestamp('QuestionDate')) / 3600
    )

    # Take the average of each tag's answer time
    avg_answer_speed = answer_speed.groupBy('Tag').agg({'AnswerSpeed': 'avg'}). \
        withColumnRenamed('avg(AnswerSpeed)', 'AvgAnswerSpeed')

    # Display results
    avg_answer_speed.show()

    # Write Results
    avg_answer_speed.write.mode('overwrite').parquet('output/answer-speed')

if __name__ == '__main__':
    input_path_tag_questions = sys.argv[1]
    input_path_answers = sys.argv[2]
    spark = SparkSession.builder.appName(
        'Answer Speed').getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    calc_answer_speed(input_path_tag_questions, input_path_answers)

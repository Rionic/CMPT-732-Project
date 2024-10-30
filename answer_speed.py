from pyspark.sql import functions as F

# Speed of Answers Based on Tags: Calculate the time it took for questions with certain
# tags to receive an answer and analyze the response times based on tags

# Some questions
# How many upvotes are required to be considered an answer?
def calc_answer_speed(q_df, a_df, t_df):
    # Get top 10 tags
    top_t = t_df.groupBy('Tag').agg({'Tag': 'count'}).sort(F.desc('count(Tag)')).limit(10)
    top_t = t_df.join(top_t, 'Tag').select('Id', 'Tag')
    
    # Join top 10 tags with questions df to get t_q df
    top_t_q = top_t.join(q_df, 'Id').select('Id', 'Tag', 'CreationDate'). \
        withColumnRenamed('CreationDate', 'QuestionDate'). \
        withColumnRenamed('Id', 'QuestionId')
        
    # Join t_q df with answers df to get t_q_a df
    top_t_q_a = top_t_q.join(a_df, top_t_q['QuestionId'] == a_df['ParentId']). \
        select('QuestionId', 'Tag', 'QuestionDate', 'CreationDate', 'Score'). \
        withColumnRenamed('CreationDate', 'AnswerDate')
        
    # We will not consider responses with score < 3 as an answer
    top_t_q_a = top_t_q_a.filter('Score > 2')
    
    # Take the date of the first answer
    fastest_answer = top_t_q_a.groupBy('QuestionId').agg({'AnswerDate': 'min'}). \
        withColumnRenamed('min(AnswerDate)', 'AnswerDate')
    
    # Join the fastest_answer with top_t_q_a to get the answer
    answer = fastest_answer.join(top_t_q_a, ['QuestionId', 'AnswerDate'])
    
    # Drop duplicates in case of a tie
    answer = answer.dropDuplicates(['QuestionId', 'AnswerDate', 'Tag'])

    
    # Get the time taken for each answer
    answer_speed = answer.withColumn(
        'AnswerSpeed',  (F.unix_timestamp('AnswerDate') - F.unix_timestamp('QuestionDate')) / 3600
    )
    
    # Take the average of each tag's answer time
    avg_answer_speed = answer_speed.groupBy('Tag').agg({'AnswerSpeed': 'avg'}). \
        withColumnRenamed('avg(AnswerSpeed)', 'AvgAnswerSpeed')
    
    # Display results
    avg_answer_speed.show()


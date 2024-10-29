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
        
    # Get the max score for each post, and join it back to t_q_a df to get max score per post
    top_ans = top_t_q_a.groupBy('QuestionId').agg({'Score': 'max'}).withColumnRenamed('max(Score)', 'Score')
    max_score_post = top_ans.join(top_t_q_a, ['QuestionId', 'Score'])
    
    # Drop duplicates in case of a tie
    max_score_post = max_score_post.dropDuplicates(['QuestionId', 'Score'])
    
    # We will not consider responses with score < 3 as an answer
    max_score_post =  max_score_post.filter('Score > 2') 
    
    # Get the time taken for each answer
    answer_speed = max_score_post.withColumn(
        'AnswerSpeed',  (F.unix_timestamp('AnswerDate') - F.unix_timestamp('QuestionDate')) / 3600
    )
    # Take the average of each tag's answer time
    avg_answer_speed = answer_speed.groupBy('Tag').agg({'AnswerSpeed': 'avg'}). \
        withColumnRenamed('avg(AnswerSpeed)', 'AvgAnswerSpeed')
    
    # Display results
    avg_answer_speed.show()


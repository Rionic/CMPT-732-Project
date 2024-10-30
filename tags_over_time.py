import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types

def analyze_top_tags_by_year(output):
    questions_df = spark.read.parquet(f"{output}/questions")
    tags_df = spark.read.parquet(f"{output}/tags")
    questions_df = questions_df.select('Id', functions.year('CreationDate').alias('year'))
    joined_df = questions_df.join(tags_df, on='Id')
    tag_counts_df = joined_df.groupBy('year', 'Tag').count().withColumnRenamed("count", "# of questions")
    top_tags = tag_counts_df.groupBy('Tag').sum("# of questions").orderBy('sum(# of questions)', ascending=False).limit(10)
    top_tag_counts_df = tag_counts_df.join(top_tags.select('Tag'), on='Tag')
    top_tag_counts_df.orderBy('year', '# of questions', ascending=False).show(100)

def analyze_tag_monthly_usage(output, tag):
    questions_df = spark.read.parquet(f"{output}/questions")
    tags_df = spark.read.parquet(f"{output}/tags")
    questions_df = questions_df.select('Id', functions.year('CreationDate').alias('year'), functions.month('CreationDate').alias('month'))
    joined_df = questions_df.join(tags_df, on='Id')
    filtered_df = joined_df.filter(joined_df.Tag == tag)
    filtered_df.show()
    monthly_counts_df = filtered_df.groupBy('year', 'month').count().withColumnRenamed("count", "# of questions")
    monthly_counts_df.orderBy('year', 'month').show(100)
# def main(output):
#     questions_df = spark.read.parquet(f"{output}/questions")
#     tags_df = spark.read.parquet(f"{output}/tags")
#     questions_df = questions_df.select('Id', 'CreationDate')
#     joined_df = questions_df.join(tags_df, on='Id')
#     joined_df.show(10)


if __name__ == '__main__':
    output = sys.argv[1]  # The output directory for Parquet files
    tag = sys.argv[2]     # The specific tag to analyze for monthly usage
    
    spark = SparkSession.builder.appName('S').getOrCreate()
    assert spark.version >= '3.0'  # Ensure Spark version is 3.0 or higher
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    arsh(output)
    # analyze_top_tags_by_year(output)  # Analyze top tags by year
    # analyze_tag_monthly_usage(output, tag)  # Analyze monthly usage for the specified tag

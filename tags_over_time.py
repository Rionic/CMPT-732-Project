from pyspark.sql import SparkSession, functions, types
import sys
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+


def analyze_top_tags_by_year(input, output):
    questions_df = spark.read.parquet(input)

    # Select the year and Tag column
    questions_df = questions_df.select(
        'Id',
        functions.year('CreationDate').alias('year'),
        'Tag'
    )

    # Count occurrences of each tag per year
    tag_counts_df = questions_df.groupBy('year', 'Tag').count(
    ).withColumnRenamed("count", "# of questions")

    # Get the top 10 tags across all years
    top_tags = tag_counts_df.groupBy('Tag').sum("# of questions").orderBy(
        'sum(# of questions)', ascending=False).limit(10)

    # Filter yearly tag counts to only include the top tags
    top_tag_counts_df = tag_counts_df.join(top_tags.select('Tag'), on='Tag')

    # Display results
    top_tag_counts_df.orderBy('year', '# of questions', ascending=False).show()

    # Order results by year and # of questions
    top_tag_counts_df.orderBy('year', '# of questions', ascending=False) \
                     .write.mode('overwrite').parquet(f"{output}/top_tags_by_year")


def analyze_tag_monthly_usage(input, output):
    # Load questions data
    questions_df = spark.read.parquet(input)

    # Select necessary columns and extract year and month
    questions_df = questions_df.select(
        'Id', 'Tag',
        functions.year('CreationDate').alias('year'),
        functions.month('CreationDate').alias('month')
    )

    # Calculate the total count of questions for each tag to find the top 10 tags
    top_tags_df = questions_df.groupBy('Tag').count().orderBy(
        'count', ascending=False).limit(10)

    # Join questions_df with top_tags to filter only rows with top tags
    top_tags_monthly_df = questions_df.join(top_tags_df, on='Tag')

    # Group by tag, year, and month and count occurrences
    monthly_counts_df = top_tags_monthly_df.groupBy(
        'Tag', 'year', 'month').count().withColumnRenamed("count", "# of questions")

    # Display results
    monthly_counts_df.orderBy('Tag', 'year', 'month').show()

    # Order results by tag, year, and month
    monthly_counts_df.orderBy('Tag', 'year', 'month') \
                     .write.mode('overwrite').parquet(f"{output}/monthly_tag_usage")


if __name__ == '__main__':
    input = sys.argv[1]  # The input directory for Parquet files
    output = sys.argv[2]  # output directory for completed analysis
    spark = SparkSession.builder.appName(
        'Final Project: Tags Over Time').getOrCreate()
    assert spark.version >= '3.0'  # Ensure Spark version is 3.0 or higher
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    analyze_top_tags_by_year(input, output)  # Analyze top tags by year
    # Analyze monthly usage for the specified tag
    analyze_tag_monthly_usage(input, output)

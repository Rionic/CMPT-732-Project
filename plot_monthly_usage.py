from pyspark.sql import SparkSession, functions as F
import matplotlib.pyplot as plt
import sys
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+


def main(input):
    # Load Data
    input_path = f"{input}/monthly_tag_usage"
    data_df = spark.read.parquet(input_path)

    # Define slopes and intercepts for each tag
    coefficients = {
        "android": (-7.54, 984.90),
        "c#": (-13.02, 1108.54),
        "c++": (-4.59, 511.68),
        "html": (-8.88, 654.23),
        "ios": (-2.59, 507.42),
        "java": (-17.67, 1281.33),
        "javascript": (-18.21, 1374.03),
        "jquery": (-12.32, 874.78),
        "php": (-16.28, 1105.69),
        "python": (-8.71, 709.93)
    }

    # Add regression predictions

    def calculate_regression(tag, month):
        slope, intercept = coefficients.get(tag.lower(), (0, 0))
        return slope * month + intercept

    calculate_regression_udf = F.udf(calculate_regression, "double")

    data_df = data_df.withColumn(
        "Regression",
        calculate_regression_udf(F.col("Tag"), F.col("month"))
    )

    data_pd = data_df.toPandas()

    # Aggregate data
    averaged_data = data_pd.groupby(['Tag', 'month'], as_index=False)[
        '# of questions'].mean()

    # Plot the data
    plt.figure(figsize=(12, 8))
    tags = averaged_data['Tag'].unique()

    for tag in tags:
        # Extract averaged data for the current tag
        tag_data = averaged_data[averaged_data['Tag'] == tag]
        months = tag_data['month']
        avg_questions = tag_data['# of questions']
        slope, intercept = coefficients.get(tag.lower(), (0, 0))
        regression_line = slope * months + intercept
        plt.scatter(
            months, avg_questions, label=f"{tag} Data (Averaged)", alpha=0.8, s=50, marker="x"
        )
        plt.plot(
            months, regression_line, linestyle="--", linewidth=2, label=f"{tag} Regression"
        )

    # Title and labels
    plt.title("Linear Regression for Monthly Tag Usage (Averaged)", fontsize=16)
    plt.xlabel("Month", fontsize=14)
    plt.ylabel("Average # Of Questions", fontsize=14)

    # Legend placement
    plt.legend(
        loc="upper left", bbox_to_anchor=(1.05, 1), fontsize=10, frameon=True, borderpad=1
    )

    # Grid and layout adjustments
    plt.grid(visible=True, linestyle="--", linewidth=0.5)
    plt.tight_layout()

    # Save and show the plot
    output_plot_path = "output_averaged_plot_with_x.png"
    plt.savefig(output_plot_path, dpi=300)
    plt.show()


if __name__ == '__main__':
    input = sys.argv[1]
    spark = SparkSession.builder.appName(
        "Monthly Tag Usage Regression").getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(input)

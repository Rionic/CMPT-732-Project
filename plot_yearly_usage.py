from pyspark.sql import SparkSession, functions as F
import matplotlib.pyplot as plt
import pandas as pd

# Define slopes and intercepts for each tag
coefficients_by_year = {
    "android": (2922.68, -5869645.82),
    "c#": (2358.00, -4732187.50),
    "c++": (1257.05, -2523407.04),
    "html": (1988.17, -3993160.00),
    "ios": (1732.46, -3480003.79),
    "java": (3553.93, -7136682.32),
    "javascript": (4107.00, -8248787.00),
    "jquery": (2305.49, -4628984.68),
    "php": (2859.76, -5741964.07),
    "python": (2042.76, -4102704.82),
}
spark = SparkSession.builder.appName("Yearly Tag Usage Regression").getOrCreate()

# Load the data
input_path = "output/top_tags_by_year"
data_df = spark.read.parquet(input_path)
data_df.show()

# Convert Spark DataFrame to Pandas
data_pd = data_df.toPandas()

# Convert column names
data_pd.rename(
    columns={"year": "year", "Tag": "Tag", "# of questions": "# of questions"},
    inplace=True,
)

# Aggregate data by averaging 'Average # Of Questions' for each year and tag
averaged_data_by_year = data_pd.groupby(['Tag', 'year'], as_index=False)['# of questions'].mean()

# Plot the data
plt.figure(figsize=(12, 8))
tags_by_year = averaged_data_by_year['Tag'].unique()

for tag in tags_by_year:
    tag_data = averaged_data_by_year[averaged_data_by_year['Tag'] == tag]
    years = tag_data['year']
    avg_questions = tag_data['# of questions']
    slope, intercept = coefficients_by_year.get(tag.lower(), (0, 0))
    regression_line = slope * years + intercept
    plt.scatter(
        years, avg_questions, label=f"{tag} Data (Averaged)", alpha=0.8, s=50, marker="x"
    )
    plt.plot(
        years, regression_line, linestyle="--", linewidth=2, label=f"{tag} Regression"
    )
plt.title("Linear Regression for Yearly Tag Usage (Averaged)", fontsize=16)
plt.xlabel("Year", fontsize=14)
plt.ylabel("Average # Of Questions", fontsize=14)
plt.legend(
    loc="upper left", bbox_to_anchor=(1.05, 1), fontsize=10, frameon=True, borderpad=1
)
plt.grid(visible=True, linestyle="--", linewidth=0.5)
plt.tight_layout()

# Save and show the plot
output_plot_path = "output_top_tags_by_year.png"
plt.savefig(output_plot_path, dpi=300)
plt.show()

# Stop Spark session
spark.stop()

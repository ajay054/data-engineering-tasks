from pyspark.sql import SparkSession
from processor import calculate_top_10_movies, get_credited_persons

def main():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("IMDB Streaming Application") \
        .master("local[*]") \
        .getOrCreate()

    # Load datasets
    title_basics_path = "data/title.basics.tsv"
    title_ratings_path = "data/title.ratings.tsv"
    title_principals_path = "data/title.principals.tsv"

    title_basics_df = spark.read.csv(title_basics_path, sep="\t", header=True)
    title_ratings_df = spark.read.csv(title_ratings_path, sep="\t", header=True)
    title_principals_df = spark.read.csv(title_principals_path, sep="\t", header=True)

    # Step 1: Calculate top 10 movies
    print("Calculating top 10 movies...")
    top_10_movies = calculate_top_10_movies(title_basics_df, title_ratings_df, spark)
    top_10_movies.show(truncate=False)

    # Step 2: Get credited persons for top 10 movies
    print("Fetching credited persons...")
    credited_persons = get_credited_persons(top_10_movies, title_principals_df)
    credited_persons.show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()

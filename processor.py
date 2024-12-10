from pyspark.sql import functions as F

def calculate_top_10_movies(title_basics_df, title_ratings_df, spark):
    """
    Calculate the top 10 movies based on the ranking formula:
    (numVotes / averageNumberOfVotes) * averageRating
    """
    # Filter for movies
    title_basics_df = title_basics_df.filter(F.col("titleType") == "movie")

    # Join datasets
    movies_with_ratings_df = title_basics_df.join(title_ratings_df, on="tconst", how="inner")

    # Filter movies with at least 500 votes
    movies_with_ratings_df = movies_with_ratings_df.filter(F.col("numVotes").cast("int") >= 500)

    # Calculate the average number of votes
    avg_num_votes = movies_with_ratings_df.select(F.avg(F.col("numVotes").cast("int")).alias("avg_votes")) \
        .collect()[0]["avg_votes"]

    # Apply ranking formula
    ranked_movies_df = movies_with_ratings_df.withColumn(
        "ranking_score",
        (F.col("numVotes").cast("int") / avg_num_votes) * F.col("averageRating").cast("float")
    )

    # Select top 10 movies
    top_10_movies = ranked_movies_df.orderBy(F.col("ranking_score").desc()).limit(10)
    return top_10_movies


def get_credited_persons(top_10_movies, title_principals_df):
    """
    List the persons credited for the top 10 movies and their roles.
    """
    # Extract tconst IDs of top 10 movies
    top_10_tconsts = [row["tconst"] for row in top_10_movies.select("tconst").collect()]

    # Filter title_principals for top 10 movies
    credited_persons_df = title_principals_df.filter(F.col("tconst").isin(top_10_tconsts))

    # Group by person (nconst) and aggregate roles and movie titles
    credited_persons_summary = credited_persons_df.groupBy("nconst").agg(
        F.collect_list("category").alias("roles"),
        F.collect_list("tconst").alias("titles")
    )

    return credited_persons_summary

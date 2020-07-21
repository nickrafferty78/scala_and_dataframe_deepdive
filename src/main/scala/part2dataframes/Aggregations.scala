package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Aggregations extends App {

    val spark = SparkSession
      .builder()
      .appName("Aggregations")
      .master("local[*]")
      .getOrCreate()

    val moviesDF = spark
      .read
      .option("inferSchema", "true")
      .json("src/main/resources/data/movies.json")

  /**
    * Counting
    */
    val genresCountDF = moviesDF
      .select(count(col("Major_Genre")))
      moviesDF.selectExpr("count(Major_Genre)")
    genresCountDF.show()

    moviesDF.select(countDistinct(col("Major_Genre")))

    //approximate count
    moviesDF.select(approx_count_distinct(col("Major_Genre")))

    val minRating = moviesDF.select(min(col("IMDB_Rating")))

    val avgRating = moviesDF.select(avg(col("Rotten_Tomatoes_Rating")))
  /**
    * Sum, average, min, max, etc are all available in spark sql functions
    */

    //data science
    moviesDF.select(
      mean(col("Rotten_Tomatoes_Rating")),
      stddev(col("Rotten_Tomatoes_Rating"))
    )

    //Grouping
    val countByGenre = moviesDF.groupBy(col("Major_Genre"))
      .count()//select count (*) from moviesDF group by Major_Genre

    val avgRatingByGenre = moviesDF.groupBy(col("Major_Genre"))
      .avg("IMDB_Rating")



    val aggByGenre = moviesDF
      .groupBy("Major_Genre")
      .agg(
        count("*").as("N_Movies"),
        avg("IMDB_Rating").as("Avg_Rating")
      )
      .orderBy("Avg_Rating").show()

  /**
    * 1. Sum up all profits of all the movies in the Dataframe
    * 2. Count how many distinct directors we have
    * 3 Show mean and std dev of US_Gross revenue
    * 4. Compute avg IMDB rating and avg us gross revenue per director
    */

    //1.
    val sumOfProfits = moviesDF
        .agg(
        sum(
        col("US_Gross")+
        col("Worldwide_Gross")+
        col("US_DVD_Sales")
        ).as("Total Sales")
      )

    //2
    val distinctDirectors = moviesDF
      .select(countDistinct(col("Director")))

  //3 Mean and std Dev
    val meanAndSTDDev = moviesDF
      .select(
        mean("US_Gross").as("Mean"),
        stddev("US_Gross").as("StdDev")
      )

  //4
    val avgIMDBPerDirector = moviesDF
      .groupBy("Director")
      .agg(
        avg("IMDB_Rating").as("Avg_Rating"),
        sum("US_Gross")
      )
      .orderBy(col("Avg_Rating").desc)
      .show()
}

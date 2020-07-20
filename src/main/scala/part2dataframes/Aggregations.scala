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
      .show()

}

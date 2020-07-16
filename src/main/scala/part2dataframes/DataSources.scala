package part2dataframes

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import part2dataframes.DataFramesBasics.{carsSchema, spark}

object DataSources extends App {

  val spark = SparkSession.builder()
    .appName("DataSources")
    .master("local[*]")
    .getOrCreate()


  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", DateType),
    StructField("Origin", StringType)
  ))

  /**
    * Requires a format
    * Requires a schema
    * Zero or more options
    */
  val carsWithSchema = spark.read
    .format("json")
    .schema(carsSchema)
    .option("mode", "failFast") //dropMalformed, Permissive (if you don't specify)
    .load("src/main/resources/data/cars.json")

  //alternative way to write it
  val carsDFWithOptionMap = spark.read
    .format("json")
    .options(Map(
      "mode"-> "failFast",
      "inferSchema" -> "true"
    ))
    .load("src/main/resources/data/cars.json")


  /**
    * Writing dataframe
    * Setting a format
    * Setting save mode if file already exists = overrite, append, ignore
    * path
    */

  carsWithSchema.write
    .format("json")
    .mode(SaveMode.Overwrite)
    .option("path", "src/main/resources/data/cars_dupe.json")
    //.save()

  //json flags
  spark.read
    .format("json")
    .schema(carsSchema)
    .option("dateFormat", "YYYY-MM-dd")//if spark fails parsing, it will put null. Be careful of this.
    .option("allowSingleQuotes", "true")
    .option("compression", "uncompressed")//bzip2, gzip, lz4, snappy, deflate - spark is able to compress automatically
    .load("src/main/resources/data/cars.json")

  //CSV's
  //csv's can have a lot of issues, it has the most flags to help you read csv's
  val stocksSchema = StructType(Array(
    StructField("symbol", StringType),
    StructField("date", DateType),
    StructField("price", DoubleType)
  ))

  spark.read
    .schema(stocksSchema)
    .format("csv")
    .option("dateFormat", "MMM dd YYYY")
    .option("header", "true")
    .option("sep", ",")//specify what separates
    .option("nullValue", "")//instructs spark to parse null if there is empty string in CSV
    .load("src/main/resources/data/stocks.csv")


  //PARQUET- Default storage format for spark
  carsWithSchema.write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/data/cars.parquet")//compresses automatically

  //TextFiles - Easiest
  spark.read
    .text("src/main/resources/data/sample_text.txt")
    .show()

  //Reading from a remote Database
//  val employeesDf = spark.read
//    .format("jdbc")
//    .option("driver", "org.postgresql.Driver")
//    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
//    .option("user", "docker")
//    .option("password", "docker")
//    .option("dbtable", "public.employees")
//    .load()

 // employeesDf.show()

  /**
    * Read movies dataframe - movies.json
    * Write it as:
    * 1. Tab separated values file (CSV)
    * 2. Snappy Parquet File
    * 3. Table in the postgresDatabase
    */

  val moviesSchema = StructType(Array(
    StructField("Title", StringType),
    StructField("US_Gross", IntegerType),
    StructField("Worldwide_Gross", IntegerType),
    StructField("US_DVD_Sales", IntegerType),
    StructField("Production_Budget", IntegerType),
    StructField("Release_Date", DateType),
    StructField("MPAA_Rating", StringType),
    StructField("Running_Time_min", IntegerType),
    StructField("Distributor",StringType),
    StructField("Source", StringType),
    StructField("Major_Genre", StringType),
    StructField("Creative_Type", StringType),
    StructField("Director", StringType),
    StructField("Rotten_Tomatoes_Rating", IntegerType),
    StructField("IMDB_Rating", DoubleType),
    StructField("IMDB_Votes", IntegerType)
  ))

  val movieDF = spark.read
    .format("json")
    .schema(moviesSchema)
    .option("dateFormat", "dd-MMM-YY")
    .load("src/main/resources/data/movies.json")

  movieDF.write
    //.csv("src/main/resources/data/movies.csv")

  movieDF.write
    //.parquet("src/main/resources/data/movies.parquet")

//  movieDF.write
//    .format("jdbc")
//    .option("driver", "org.postgresql.Driver")
//    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
//    .option("user", "docker")
//    .option("password", "docker")
//    .option("dbtable", "public.movies")
//    .save()


  /**
    * 1.Read movies df and select two columns of choice
    * 2. Create a new df by summing up all gross profits - US dvd sales, us and world gross
    * 3. Select all COMEDY movies with IMDB above 6
    *
    * Use as many versions as possible
    */

  val moviesDF = spark.read
    .schema(moviesSchema)
    .format("json")
    .option("dateFormat", "dd-MMM-YY")
    .load("src/main/resources/data/movies.json")
  val newDF = moviesDF.select(col("Title"), col("Major_Genre"))
  newDF.show()

  //2.
  val sumOfProfits = moviesDF.select(
    col("Title"),
    col("US_Gross"),
    col("Worldwide_Gross"),
    col("US_DVD_Sales"),
    (col("US_Gross") + col("Worldwide_Gross")).as("TotalSales")
  ).show(100)

  val sumOfProfitsTwo = moviesDF.selectExpr(
    "Title",
    "US_Gross",
    "WorldWide_Gross",
    "US_Gross + Worldwide_Gross as TotalGross"
  ).show()


  val selectGoodMovies = moviesDF.select("Title", "IMDB_Rating")
    .where(
      col("Major_Genre") === "Comedy"
      and
      col("IMDB_Rating") > 6
    )
    .show()

}

package part2dataframes

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

object DataFramesBasics extends App {

    val spark = SparkSession.builder()
                .appName("DataFramesBasics")
                .config("spark.master", "local")
                .getOrCreate()

    //reading a dataframe
    val firstDataFrame = spark
      .read
      .format("json")

      /**
        * This part should not be used in production, you should define your own schema
        */
      .option("inferSchema", "true")
      .load("src/main/resources/data/cars.json")

    //show dataframe
    firstDataFrame.show()
    firstDataFrame.printSchema()


    firstDataFrame.take(10).foreach(println)

    //spark type
    val longType = LongType

  /**
    * A Spark schema structure that describes a small cars DataFrame.
    */
  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", StringType),
    StructField("Origin", StringType)
  ))

  val carsDFSchema = firstDataFrame.schema

    val carsWithSchema = spark.read
      .format("json")
      .schema(carsSchema)
      .load("src/main/resources/data/cars.json")

    //create rows by hand
    val myRow =  Row("chevrolet chevelle malibu",18.0,8L,307.0,130L,3504L,12.0,"1970-01-01","USA")

    //create df from seq
    val cars = Seq(
      ("chevrolet chevelle malibu",18.0,8L,307.0,130L,3504L,12.0,"1970-01-01","USA"),
      ("buick skylark 320",15.0,8L,350.0,165L,3693L,11.5,"1970-01-01","USA"),
      ("plymouth satellite",18.0,8L,318.0,150L,3436L,11.0,"1970-01-01","USA"),
      ("amc rebel sst",16.0,8L,304.0,150L,3433L,12.0,"1970-01-01","USA"),
      ("ford torino",17.0,8L,302.0,140L,3449L,10.5,"1970-01-01","USA"),
      ("ford galaxie 500",15.0,8L,429.0,198L,4341L,10.0,"1970-01-01","USA"),
      ("chevrolet impala",14.0,8L,454.0,220L,4354L,9.0,"1970-01-01","USA"),
      ("plymouth fury iii",14.0,8L,440.0,215L,4312L,8.5,"1970-01-01","USA"),
      ("pontiac catalina",14.0,8L,455.0,225L,4425L,10.0,"1970-01-01","USA"),
      ("amc ambassador dpl",15.0,8L,390.0,190L,3850L,8.5,"1970-01-01","USA")
    )
    val manualCarsDataFrame = spark.createDataFrame(cars)
    //create df's with impplicits - can put column names in
  import spark.implicits._
  val manualCarsWithImplicits = cars.toDF()
  manualCarsDataFrame.printSchema()
  manualCarsWithImplicits.printSchema()

  /**
    * Exercises
    * 1)Create a manual dataframe describing smartphones
    * -make, model, etc
    *
    * 2) Read another file from data folder, movies.json
    * -printshema
    * -count number of rows
    */

  val smartPhones = Seq(
    ("Apple", 560.1, "11 X", "The newest iphone available", 3),
    ("Apple", 605.0, "10 X", "The most powerful iphone", 2),
    ("Android", 400.9, "3", "Android's galaxy 3", 3),
    ("Blackberry", 200.1, "Notepad", "Blackberry service", 3)
  )
  val createManualSmartPhoneDF = spark.createDataFrame(smartPhones)
  val createSmartPhoneDFWithImplicits = smartPhones.toDF("Make", "Price", "Model", "Description", "Warranty")
  createManualSmartPhoneDF.show()
  createSmartPhoneDFWithImplicits.show()


  /**
    * Exercise two
    */

  val moviesDF = spark.read.format("json").load("src/main/resources/data/movies.json")
  moviesDF.printSchema()
 println( moviesDF.count())

}

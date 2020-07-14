package part2dataframes



import Utilities._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, column, expr}
import org.slf4j.Logger




object ColumnsAndExpressions extends App {



    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("DF Columns and Expressions")
      .getOrCreate()

    val carsDf = spark.read
      .option("inferSchema", "true")
      .json("src/main/resources/data/cars.json")

    carsDf.show()

    //Columns
    val firstColumn = carsDf.col("Name")

    //Select
    val carNames = carsDf.select(firstColumn)

    carNames.show
import spark.implicits._
    //various select methods
    carsDf.select(
      carsDf.col("Name"),
      carsDf.col("Acceleration"),
        column("Weight_in_lbs"),
        'Year,
        $"Horsepower",//returns column object
        expr("Origin")
    )

    carsDf.select("Name", "Year")//simplest versions of expressions

    //Expressions
    val simplestExpression = carsDf.col("Weight_in_lbs")
    val weightInKG = carsDf.col("Weight_in_lbs") /2.2

    val carsWithWeightsDf = carsDf.select(
        col("Name"),
        col("Weight_in_lbs"),
        weightInKG.as("Weight In KG")
    )
    carsWithWeightsDf.show()

    val carsWithSelectExp = carsDf.selectExpr(
        "Name",
        "Weight_in_lbs",
        "Weight_in_lbs/2.2"
    )

    //DF Processing
    //adding a column
//    carsDf.withColumn("WeightInKG", col("Weight_in_lbs/2.2"))

    val columnsRenamed = carsDf.withColumnRenamed("Weight_in_lbs", "Weight In Pounds")
    //careful with spaces
    columnsRenamed.selectExpr("'Weight In Pounds'")
    //remove column
    columnsRenamed.drop("Cylinders", "Replacement")

    val europeanCars = carsDf.filter(col("Origin")=!= "USA")
    //same thing
    val europeanCarsDf = carsDf.where(col("Origin")=!= "USA")

    val americanPowerfulCars = carsDf.filter(col("Origin")=== "USA").filter(col("Horsepower") >150 )

    //unioning = adding more rows
    val addedMoreCarsDf = spark.read
      .option("inferSchema", "true")
      .json("src/main/resources/data/more_cars.json")

    val allCarsDf = carsDf.union(addedMoreCarsDf)//have to have same schema


    //distinct
    val allCountries = carsDf.select(col("Origin")).distinct()
    allCountries.show()
}

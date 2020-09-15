package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{expr, max, col}

object Joins extends App {

  val spark = SparkSession.builder()
    .appName("Joins")
    .master("local[*]")
    .getOrCreate()

  val employeesDF = spark.read
    .format("jdbc")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("driver", "org.postgresql.Driver")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", "employees")
    .load()

//  val bands = spark.read
//    .option("inferSchema", "true")
//    .json("src/main/resources/data/bands.json")
//
//  val guitarPlayers = spark.read
//    .option("inferSchema", "true")
//    .json("src/main/resources/data/guitarPlayers.json")
//
//  val joinConditions = guitarPlayers.col("band") === bands.col("id")
//  val guitaristBands = guitarPlayers.join(bands, joinConditions, "inner")
//
//  //joins!
//  //inner - everything that is matched in the two tables based on the conditions
//  //left outer - inner join plus everything from left (guitar players) table. It will fill in nulls for the right table
//  //right outer- opposite
//  //outer join - everything in all tables
//  //left_semi - inner join but cut out data from right table
//  //left_anti - the missing row from left dataframe, everything is null in right table
//
//  val guitaristBandsLeftOuter = guitarPlayers.join(bands, joinConditions, "left_anti")
//
//  val columnsRenamed = guitarPlayers.join(bands, joinConditions).drop(bands.col("id"))
//
//  //using complex types
//  val arrayJoins = guitarPlayers.join(guitars.withColumnRenamed("id", "guitar_id"), expr("array_contains(guitars, guitar_id)"))

  /**
    * Show all employees and their max salaries
    * Show all employees who were never managers
    * find the job titles of the best paid 10 employees
    */
    val salariesDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
      .option("driver", "org.postgresql.Driver")
      .option("user", "docker")
      .option("password", "docker")
      .option("dbtable", "salaries")
      .load()

  //Show all employees and max salaries
  val employees = employeesDF.join(salariesDF, salariesDF.col("emp_no") === employeesDF.col("emp_no"), "inner").drop(salariesDF.col("emp_no"))
  employees.show()
}


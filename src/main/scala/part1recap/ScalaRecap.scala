package part1recap

import scala.concurrent.Future
import scala.util.{Failure, Success}

object ScalaRecap extends App {

  // values and variables
  val aBoolean: Boolean = true
  val andIfexpression = if(2>3) "bigger" else "smaller"
  val theUnit = println("hello") //no meaningful value
  def functionName(x:Int): Unit ={
    42
  }

  //oop
  class Animal
  class Cat extends Animal
  trait Carnivore {
    def eat(animal:Animal): Unit
  }

  class Crocodile extends Animal with Carnivore {
    def eat(animal:Animal): Unit=println("crunch")
  }
  //singleton
  object MySingleton{

  }

  object Carnivore

  //generics
  trait MyList[A]

//method notation
  val x= 1+2
  val y = 1.+(2)

  //Functional Programming
  val incrementer: Int => Int = x => x+1
  val incremented = incrementer(42)

  //map, flatmap and filter - all higher order functions, they can take in other functions as paramters

  //Pattern matching
  val unknown:Any = 45
  val ordinal = unknown match {
    case 1 => "first"
    case 2 => "second"
    case _ => "unknown"
  }
  //try catch
  try{
    throw new NullPointerException
  }catch{
    case e: NullPointerException => "Some value"
    case _ => "something else"
  }

  //Futures
  import scala.concurrent.ExecutionContext.Implicits.global
  val aFuture = Future {
    //some expensive computation, runs on another thread
    42
  }
  aFuture.onComplete {
    case Success(meaningOfLife) => println(s"Ive found the $meaningOfLife")
    case Failure(ex) => println(s"oh no $ex")
  }

  //partial functions
  val aPartialFunction: PartialFunction[Int, Int] = {
    case 1 => 34
    case _ => 99
  }

  //implicits - one of the most important part
  //auto injected into the compiler
  def methodwithImplicit(implicit x:Int) = x +43
  implicit val implicitInt = 67
//implicit call
  methodwithImplicit

  //implicit conversions
  case class Person(name:String) {
    def greet = println(s"Hi my name is $name")
  }
  implicit def fromStringToPerson(name:String) = Person(name)
  "Bob".greet

  //implicit conversion with implicit classes
  implicit class Dog(name:String){
    def bark = println("Bark")
  }

  "Lassie".bark



}

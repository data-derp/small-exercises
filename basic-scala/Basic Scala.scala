// Databricks notebook source
// MAGIC %md
// MAGIC # Basic Scala
// MAGIC This notebook contains a few helpful Scala functions in preparation for your first Scala notebook.

// COMMAND ----------

// MAGIC %md
// MAGIC ## If Statements
// MAGIC 
// MAGIC [Want to see more?](https://docs.scala-lang.org/overviews/scala-book/if-then-else-construct.html)

// COMMAND ----------

// Let's declare ourselves a method that contains an if statement. Let's observe that you have a and b as inputs and an output type of Unit

def myAwesomeIfStatement(a: Int, b: Int): Unit =
  if (a == b) {
    println("These are equal")
  } else {
    println("These are not equal")
  }

// COMMAND ----------

// Let's call that on a and b (both equalling 1)
myAwesomeIfStatement(a = 1, b = 1)

// COMMAND ----------

// What happens when a and b are not equal?
myAwesomeIfStatement(a = 1, b = 2)

// COMMAND ----------

// You can also write this as a one-liner
if (1 == 1) println("These are equal")

// COMMAND ----------

// What happens if they're not equal?
if (1 == 2) println("These are equal")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Case Matches
// MAGIC 
// MAGIC [Want to see more?](https://docs.scala-lang.org/overviews/scala-book/match-expressions.html)

// COMMAND ----------

// Let's define a case statement

def myAwesomeMatchStatement(i: Int): String = 
  i match {
      case 1  => "January"
      case 2  => "February"
      case 3  => "March"
      case 4  => "April"
      case 5  => "May"
      case 6  => "June"
      case 7  => "July"
      case 8  => "August"
      case 9  => "September"
      case 10 => "October"
      case 11 => "November"
      case 12 => "December"
      case _  => "Invalid month"
  }

// COMMAND ----------

// Pass in 1, we should get "January"
myAwesomeMatchStatement(1) 

// COMMAND ----------

// Pass in 13, we should get "Invalid month"
myAwesomeMatchStatement(13) 

// COMMAND ----------

// MAGIC %md
// MAGIC ## For expressions
// MAGIC 
// MAGIC [Want to see more?](https://docs.scala-lang.org/overviews/scala-book/for-loops.html)

// COMMAND ----------

// We can iterate through a Seq using "for"
val nums = Seq(1,2,3)
for (n <- nums) println(n)

// COMMAND ----------

// We can also do this with a List
val people = List(
    "Bill", 
    "Candy", 
    "Karen", 
    "Leo", 
    "Regina"
)

for (p <- people) println(p)


// COMMAND ----------

// Do the same thing using foreach
people.foreach(println)


// COMMAND ----------

// MAGIC %md
// MAGIC # Map
// MAGIC [Reference](https://medium.com/@mallikakulkarni/functional-programming-in-scala-2-the-map-function-f9b9ee17d495)
// MAGIC 
// MAGIC Maps are incredibly powerful, especially in functional programming.

// COMMAND ----------

// We can pass in an anonymous function

val list = List(1,2,3)
val doubled = list.map(elem =>
  elem * 2
)
println(doubled) // Output List(2, 4, 6)

// COMMAND ----------

// We can also call another function within it using _ as our iterable

val list = List("one","two","three")
val lengths = list.map(_.length)
println(lengths) // Output List(3, 3, 5)

// COMMAND ----------

// We can also use a case statement in our map

val list = List("one","two","three")
val patterns = list.map({
  case "one" => Some(1)
  case "two" => Some(2)
  case _ => None
})
println(patterns) // Output List(Some(1), Some(2), None) 
println(patterns.flatten) // Output List(1, 2)

// COMMAND ----------

// We can also pass through a custom function

def mapfn(a: Int) : Int = {
  a * 2
}

def map4 = {
  val list = List(1, 2, 3)
  val doubles = list.map(mapfn)
  println(doubles)
} 

map4 // Output List(2, 4, 6)

// COMMAND ----------

// We can also chain it to a filter

def map5 = {
  val list = List(1, 2, 3)
  val doubles = list.filter(_ > 1).map(_*2)
  println(doubles)
} 

map5 // Output List(4, 6)


// COMMAND ----------



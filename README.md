# Scala Notes and Labs

* Title: Scala to use Spark 
* Author: fcbarbi@gmail.com 
* Update: July 2019  

This is the main list of classes/exercises in Scala for our course in data analysis.

## Lab 01. Setup tools, libraries and development enviornment

Check the version of Scala, as of this writing the current version is 
```
$ scalac -version
Scala compiler version 2.13.0 -- Copyright 2002-2019, LAMP/EPFL and Lightbend, Inc.
```

* TIP: Easier way to install/update using Brew: `brew update` and `bre install scala` or `brew upgrade scala`. 

* TIP: IntelliJ is a great IDE but we will use it later, now you have to know the path for manual compilation.

```
$ sbt new scala/hello-world.g8

name [Hello World template]: hello-world

$ tree 
.
└── hello-world
    ├── build.sbt
    ├── project
    │   └── build.properties
    └── src
        └── main
            └── scala
                └── Main.scala

```                
Then compile and run it with Simple Build Tool (SBT)
```
$ sbt 
sbt:hello-world> run

( ... program runs, what happens ? ... )

sbt:hello-world> exit
[info] shutting down server
```
Check the Scala code for this app:
```
$ cat src/main/scala/Main.scala 

object Main extends App {
  println("Hello, World!")
}
```

You can check more details on the interactive Scala (or REPL read–eval–print loop) at `https://www.scala-exercises.org/scala_tutorial/terms_and_types` to get a console to test your code line by line. 

```
$ scala 

scala> def presentation(name: String, age: Int): String =
"Hello, my name is " + name + ". I am " + age + " years old."

scala> presentation(age = 25, name = "Fernando")

```
BONUS POINTS: You can try this testing framework to automate tests: 
```
sbt new scala/scalatest-example.g8
```
https://docs.scala-lang.org/getting-started-sbt-track/testing-scala-with-sbt-on-the-command-line.html

## Note: Know the differences between _class_, _abstract class_ and a _trait_.

* An abstract class is a class that has only a signature for attributes and methods, no implementation. 

* You cannot instantiate an abstract class: you must create a subclass that implements all the abstract members.

* Trait is similar to an abstract class but while a given class can only extend one abstract class, it can mixin many traits.


## Lab 02. Map Reduce for counting words 

The `mapreduce.scala` 

```
package object mapreduce {

  import _root_.scala.actors.Futures._
  import _root_.scala.collection.SortedMap

  class Mappable[KEYIN, VALUEIN](mappee: Iterable[(KEYIN, VALUEIN)]) {

    def mapper[KEYOUT, VALUEOUT](mapper: (KEYIN, VALUEIN) => Iterable[(KEYOUT, VALUEOUT)])(implicit ord: Ordering[KEYOUT]) : Iterable[(KEYOUT, VALUEOUT)] = {
      mappee.map { case (key, value) => future { mapper(key, value) } }.flatMap { _() }
    }
  }

  implicit def iterable2Mappable[A, B](m: Iterable[(A, B)]) = new Mappable(m)

  class Reducable[KEYIN, VALUEIN](reducee: Iterable[(KEYIN, VALUEIN)])(implicit ord: Ordering[KEYIN]) {

    def reducer[KEYOUT, VALUEOUT](reducer: (KEYIN, Iterable[VALUEIN]) => (KEYOUT, VALUEOUT)) : Iterable[(KEYOUT, VALUEOUT)] = {
      reducee.foldLeft(SortedMap.empty[KEYIN, List[VALUEIN]](ord)) {
        case (map, (key, value)) => {
          map + (key -> (value :: map.getOrElse(key, Nil)))
        }
      }.map { case (key, values) => future { reducer(key, values) } }.map { _() }
    }
  }

  implicit def iterable2Reducable[A, B](r: Iterable[(A, B)])(implicit ord: Ordering[A]) = new Reducable(r)(ord)
}
```
A wordcount example shows how to use mapreduce():
```
object WordCount {

  def main(args: Array[String]) {

    import mapreduce._
    import _root_.scala.io.Source

    def textInputFormat(lines: Iterator[String], offset: Long = 0): Stream[(Long, String)] = {
      if(lines.hasNext) {
        val line = lines.next
        Stream.cons((offset, line), textInputFormat(lines, offset+line.length))
      }
      else {
        Stream.empty
      }
    }

    val source = Source.fromFile(args(0))
    try {
      textInputFormat(source.getLines).mapper {
        (offset, str) => {
          str.split("\\W+").collect { case word if word != "" => (word -> 1) }
        }
      }.reducer {
        (word, counts) => {
          word -> (counts.sum)
        }
      }.foreach { case (key, value) => println("%s: %d".format(key, value)) }
    } finally {
      source.close
    }
  }
}
```

[Source](https://gist.github.com/ueshin/740567)

## Note 

* Iterable 

* implicit

* flatMap

* 

* 

## Lab 03. Map Reduce 

In this lab we have to write code to get the max count for the first word in all distinct word pairs.

1. Strip punctuations, split content into words which get lowercased
2. Use sliding(2) to create array of word pairs
3. Use reduceByKey to count occurrences of distinct word pairs
4. Use reduceByKey again to capture word pairs with max count for the first word

```
import org.apache.spark.sql.functions._
import org.apache.spark.mllib.rdd.RDDFunctions._

val wordPairCountRDD = sc.textFile("/path/to/textfile").
  flatMap( _.split("""[\s,.;:!?]+""") ).
  map( _.toLowerCase ).
  sliding(2).
  map{ case Array(w1, w2) => ((w1, w2), 1) }.
  reduceByKey( _ + _ )

val wordPairMaxRDD = wordPairCountRDD.
  map{ case ((w1, w2), c) => (w1, (w2, c)) }.
  reduceByKey( (acc, x) =>
    if (x._2 > acc._2) (x._1, x._2) else acc
  ).
  map{ case (w1, (w2, c)) => ((w1, w2), c) }
```

[Source](https://stackoverflow.com/questions/49986614/mapreduce-example-in-scala)



 

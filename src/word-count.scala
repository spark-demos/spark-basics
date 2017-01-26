import org.apache.spark.sql.SparkSession

import org.apache.log4j._

Logger.getLogger("org").setLevel(Level.ERROR)

val spark = SparkSession.builder().getOrCreate()
val input = spark.sparkContext.textFile("datasets/word-count.txt")
val words = input.flatMap(line => line.split(" "))
val counts = words.map(word => (word, 1)).reduceByKey{case (x, y) => x + y}

println("Total words in file: ")
counts.foreach(println)
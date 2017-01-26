// https://www.kaggle.com/deepmatrix/imdb-5000-movie-dataset
import org.apache.spark.sql.SparkSession

import org.apache.log4j._

Logger.getLogger("org").setLevel(Level.ERROR)

val spark = SparkSession.builder().getOrCreate()
val dataFrame = (spark.read
                .option("header", "true")
                .option("inferSchema", "true")
                .format("csv").load("datasets/movie_metadata.csv"))

println(dataFrame.printSchema)

val dataRDD = dataFrame.rdd

val moviesCountByLanguage = (dataRDD
  .map(row => (row(19), 1))
  .filter(_._1 != null)
  .reduceByKey(_+_)
  .sortBy(_._2))


moviesCountByLanguage.foreach(println)

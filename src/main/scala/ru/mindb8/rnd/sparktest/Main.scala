package ru.mindb8.rnd.sparktest

import org.apache.commons.lang3.StringUtils.SPACE
import org.apache.spark.rdd.{PairRDDFunctions, RDD}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object Main {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "/C:/###/hadoop/hadoop-2.8.3/")
    if (args.length < 0) {
      System.err.println("Usage: ScalaWordCount <file>")
      System.exit(1)
    }
    val sparkConf = new SparkConf()
      .setAppName("ScalaWordCount")
      .setMaster("local")
    val ctx = new SparkContext(sparkConf)


    val spark = SparkSession
                .builder()
                .appName("Example")
                .config("spark.master", "local")
                .getOrCreate()

    spark.read.

    val lines = ctx.textFile("LICENSE", 1)
    val data = List(1, 2, 3, 4, 5)
    val distData: RDD[Int] = ctx.parallelize(data)
    val x =

   //  val words: RDD[String] = lines.flatMap((s: String) => SPACE.split(s))
  //  val ones: PairRDDFunctions[String, Integer] = words.map((word: String) => new Tuple2[String, Integer](word, 1))
  //  val counts: PairRDDFunctions[String, Integer] = ones.reduceByKey((i1: Integer, i2: Integer) => i1 + i2)

    for (word <- lines) {
      System.out.println(word)
    }

//    for (tuple <- counts.collectAsMap()) {
//      System.out.println(tuple._1 + ": " + tuple._2)
//    }
//    println("Hello, world!")
  }
}
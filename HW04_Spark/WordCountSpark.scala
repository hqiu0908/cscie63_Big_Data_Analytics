/**
 * Illustrates flatMap + countByValue for wordcount.
 */
package edu.hu.examples

import org.apache.spark._
import org.apache.spark.SparkContext._

object WordCountSpark {
    def main(args: Array[String]) {

      val inputFile = args(0)
      val outputFile = args(1)
      
      val conf = new SparkConf().setAppName("wordCount")
      
      // Create a Scala Spark Context.
      val sc = new SparkContext(conf)
      
      // Load our input data.
      val input =  sc.textFile(inputFile)
      
      // Split up into words.
      val words = input.flatMap(line => line.replaceAll("[^a-zA-Z\\d]", " ").toLowerCase().trim().split("\\s+"))

      // val words = input.flatMap(line => line.replaceAll("[.,;:$%&'+/\\*\\#\\-\\?!\\\"\\[\\]\\(\\)\\{\\}_]", "").toLowerCase().trim().split(" "))

      // Transform into word and count.
      val counts = words.map(word => (word, 1)).reduceByKey{case (x, y) => x + y}
      
      // Save the word count back out to a text file, causing evaluation.
      counts.sortByKey().saveAsTextFile(outputFile)
    }
}

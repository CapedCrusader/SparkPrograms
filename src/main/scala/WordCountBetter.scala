import org.apache.spark._
import org.apache.log4j._

object WordCountBetter {

  def main(args: Array[String]) : Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "WordCountBetter")

    val input = sc.textFile("data/Book.txt")

    val words = input.flatMap(x => x.split("\\W+"))

    val lowercasewords = words.map(x => x.toLowerCase())

    val wordcounts = lowercasewords.countByValue()

    wordcounts.foreach(println)

  }
}

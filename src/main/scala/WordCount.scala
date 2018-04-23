import org.apache.spark._
import org.apache.log4j._

object WordCount {

  def main(args: Array[String]) : Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "WordCount")

    val input = sc.textFile("data/Book.txt")

    val words = input.flatMap(x => x.split(" "))

    val wordcounts = words.countByValue()

    wordcounts.foreach(println)

  }

}

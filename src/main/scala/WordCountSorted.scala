import org.apache.spark._
import org.apache.log4j._

object WordCountSorted {

  def main(args: Array[String]) : Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "WordCountSorted")

    val input = sc.textFile("data/Book.txt")

    val words = input.flatMap(x => x.split("\\W+"))

    val lowercasewords = words.map(x => x.toLowerCase())

    val wordcounts = lowercasewords.map(x => (x,1)).reduceByKey((x,y) => x+y)

    val wordcountsorted = wordcounts.map(x => (x._2, x._1)).sortByKey()

    for(result <- wordcountsorted){
      val count = result._1
      val word  = result._2
      println(s"$word:$count")
    }


  }
}

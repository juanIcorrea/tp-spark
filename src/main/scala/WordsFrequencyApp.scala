import org.apache.spark.SparkContext

object WordsFrequencyApp {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local", "WordsFrequencyApp")

    val textFile = sc.textFile("C:/Users/Mati/Projects/Faculty/Sistemas Distribuidos/Implementation/tpspark/src/main/scala/data/books/*")

    textFile.map(_.toLowerCase)
      .flatMap(line => line.split("\\W+"))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .map(_.swap)
      .top(50)
      .map(_.swap)
      .foreach(println(_))

    sc.stop()
  }
}

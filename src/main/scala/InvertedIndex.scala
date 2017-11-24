import org.apache.spark.SparkContext

object InvertedIndex {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[*]", "InvertedIndexApp")

    val textFile = sc.wholeTextFiles("C:/Users/Mati/Projects/Faculty/Sistemas Distribuidos/Implementation/tpspark/src/main/scala/data/books/*")

    textFile.flatMap{
      case (name, content) =>
        val words = content.toLowerCase.split("""\W+""")
        words.map(word => (word,Set(name):Set[String]))
    }.reduceByKey{
      case (list1, list2) =>
        list1 ++ list2
    }.saveAsTextFile("C:/Users/Mati/Projects/Faculty/Sistemas Distribuidos/Implementation/tpspark/src/main/scala/output")

    sc.stop()
  }
}

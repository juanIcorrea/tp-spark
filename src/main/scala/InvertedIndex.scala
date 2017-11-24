import org.apache.spark.SparkContext

object InvertedIndex {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[*]", "InvertedIndexApp")

    // Se usa wholeTextFiles para obtener los nombres de los files para poder guardarlos
    val textFile = sc.wholeTextFiles("C:/Users/Mati/Projects/Faculty/Sistemas Distribuidos/Implementation/tpspark/src/main/scala/data/books/*")

    textFile.flatMap{
      case (name, content) =>
        val words = content.toLowerCase.split("\\W+")
        words.map(word => {
          val pathArray: Array[String] = name.split("/")
          (word,Set(pathArray(pathArray.length-1)):Set[String])
        })
    }.reduceByKey{
      case (list1, list2) =>
        list1 ++ list2
    }.foreach{
      case (word, files) =>

        println(word + " -> " + files)
    }

    sc.stop()
  }
}
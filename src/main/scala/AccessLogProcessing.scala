import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object AccessLogProcessing {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local", "WordsFrequencyApp")

    // RDD con las lineas del access log
    val textFile = sc.textFile("C:/Users/Mati/Projects/Faculty/Sistemas Distribuidos/Implementation/tpspark/src/main/scala/data/accessLog/access_log_Jul95")

//    mostVisited(textFile)
    requestPerHour(textFile)

    sc.stop()
  }

  def mostVisited(textFile: RDD[String]): Unit = {
    textFile.map(_.toLowerCase) // Se pone en minuscula a cada linea
      .map(line => line.split(" ")(0)) // En cada linea/request se obtiene la url
      .map(word => (word, 1)) // Te queda un RDD de tuplas siendo (url, 1)
      .reduceByKey(_ + _) // Se aplica un reduce para que quede un RDD de (url, frecuencia)
      .map(_.swap) // Cambia las tuplas para sortear por key quedando (frecuencia, url)
      .top(10) // Trae los primeros 10
      .map(_.swap) // Se vuelve a (url, frecuencia) para mostrar
      .foreach(println(_))
  }

  def requestPerHour(textFile: RDD[String]): Unit = {
    textFile.map(_.toLowerCase) // Se pone en minuscula a cada linea
      .map(line => {
      line.split(" ")(3).split(":")(1)
    }) // En cada linea/request se obtiene la hora
      .map(hour => (hour, 1)) // Te queda un RDD de tuplas siendo (hora, 1)
      .reduceByKey(_ + _) // Se aplica un reduce para que quede un RDD de (hora, frecuencia)
      .map(tuple => (tuple._1, tuple._2/31)) // Se divide la cantidad de request por los dias del log.
      .map(_.swap)
      .top(24) // Se ordena por frecuencia
      .map(_.swap)
      .foreach(println(_))
  }

}

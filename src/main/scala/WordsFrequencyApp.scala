import org.apache.spark.SparkContext

object WordsFrequencyApp {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local", "WordsFrequencyApp")

    // RDD con las lineas de todos los archivos
    val textFile = sc.textFile("C:/Users/Mati/Projects/Faculty/Sistemas Distribuidos/Implementation/tpspark/src/main/scala/data/books/*")

    textFile.map(_.toLowerCase) // Se pone en minuscula a cada linea
      .flatMap(line => line.split("\\W+")) // Se divide cada linea por palabras y como es flatmap(hace un flatten) te queda un RDD de palabras
      .map(word => (word, 1)) // Te queda un RDD de tuplas siendo (palabra, 1)
      .reduceByKey(_ + _) // Se aplica un reduce para que quede un RDD de (palabra, frecuencia)
      .map(_.swap) // Cambia las tuplas para sortear por key quedando (frecuencia, palabra)
      .top(50) // Trae los primeros 50
      .map(_.swap) // Se vuelve a (palabra, frecuencia) para mostrar
      .foreach(println(_))

    sc.stop()
  }
}

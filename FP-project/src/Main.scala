import org.apache.spark.sql.SparkSession

object MongoDBWriter {
  def main(args: Array[String]): Unit = {
    // 1. Créer une session Spark avec le connecteur MongoDB
    val spark = SparkSession.builder()
      .appName("Spark to MongoDB")
      .config("spark.mongodb.write.connection.uri", "mongodb+srv://ni3co8germani:FP-Project@cluster0.mx7jq.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
      .config("spark.mongodb.read.connection.uri", "mongodb+srv://ni3co8germani:FP-Project@cluster0.mx7jq.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
      .getOrCreate()

    // 2. Lire le fichier JSON
    val jsonFilePath = "path/to/your/file.json" // Chemin vers le fichier JSON
    val jsonData = spark.read.json(jsonFilePath)

    // 3. Afficher les données lues pour vérifier
    jsonData.show()

    // 4. Écrire les données dans MongoDB
    jsonData.write
      .format("mongodb")
      .mode("append")
      .save()

    // 5. Arrêter la session Spark
    spark.stop()
  }
}

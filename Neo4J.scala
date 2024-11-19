import org.apache.spark.sql.{SparkSession, DataFrame}
import org.neo4j.spark._
import org.apache.spark.sql.functions._

object Neo4J extends App{

  // Create a Spark session
  val spark = SparkSession.builder()
    .appName("CVE to Neo4j")
    .master("local[*]")
    .config("spark.neo4j.bolt.url", "")
    .config("spark.neo4j.bolt.user", "neo4j")
    .config("spark.neo4j.bolt.password", "")
    .getOrCreate()

  // Define the path to the folder containing JSON files
  val folderPath = "data/all-cve"

  // Read all JSON files from the folder
  val cveDF = spark.read.json(folderPath)

  // Print the schema
  cveDF.printSchema()




  // Function to write data to Neo4j
  def writeToNeo4j(df: DataFrame, labels: String, nodeKeys: String, relationship: Option[String] = None,
                   sourceLabels: Option[String] = None, targetLabels: Option[String] = None,
                   targetNodeKeys: Option[String] = None, targetNodeProperties: Option[String] = None): Unit = {
    val writer = df.write
      .format("org.neo4j.spark.DataSource")
      .mode("Append")
      .option("labels", labels)
      .option("node.keys", nodeKeys)

    relationship.foreach { rel =>
      writer
        .option("relationship", rel)
        .option("relationship.source.labels", sourceLabels.getOrElse(""))
        .option("relationship.source.save.mode", "Match")
        .option("relationship.source.node.keys", nodeKeys)
        .option("relationship.target.labels", targetLabels.getOrElse(""))
        .option("relationship.target.node.keys", targetNodeKeys.getOrElse(""))
        .option("relationship.target.node.properties", targetNodeProperties.getOrElse(""))
    }

    writer.save()
  }

  // Create Neo4j nodes for CVEs
  writeToNeo4j(cveDF.select("id"), ":CVE", "id")

  // Create Description nodes and relationships
  val descriptionDF = cveDF.select("id", "description")
    .filter(col("description").isNotNull)
    .distinct()

  writeToNeo4j(
    descriptionDF,
    ":Description",
    "description",
    Some("HAS"),
    Some(":CVE"),
    Some(":Description"),
    Some("description"),
    Some("description")
  )

  // Create ImpactScore nodes and relationships
  val impactScoreDF = cveDF.select("id", "impact.baseMetricV3.impactScore")
    .filter(col("impact.baseMetricV3.impactScore").isNotNull)
    .distinct()

  writeToNeo4j(
    impactScoreDF,
    ":ImpactScore",
    "impactScore",
    Some("HAS"),
    Some(":CVE"),
    Some(":ImpactScore"),
    Some("impactScore"),
    Some("impactScore")
  )

  // Stop the Spark session
  spark.stop()

}

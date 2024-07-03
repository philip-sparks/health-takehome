package serifHealthTakeHome

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}

object SerifQuestions extends App {
  val spark = SparkSession.builder()
    .appName("Serif Takehome Anthem Index Parsing")
    .config("spark.master", "local")
    .getOrCreate()

    // Load JSON file into DataFrame with multiLine and permissive mode options
    val df = spark.read
      .option("header", "true")
      .csv("/Users/psparks/Public/Github/serifTH/descriptions_and_locations.csv")

  df.show()
  df.printSchema()
  println(df.count())

  // Perform count distinct on each column
//  val columnNames = df.columns
//  val distinctCounts = columnNames.map { colName =>
//    (colName, df.select(colName).distinct().count())
//  }
//
//  // Print the distinct counts for each column
//  println("Distinct counts for each column:")
//  distinctCounts.foreach { case (colName, count) =>
//    println(s"$colName: $count")
//  }

//  // Define schema based on the JSON structure
//  val fileLocationSchema = StructType(Array(
//    StructField("description", StringType, nullable = true),
//    StructField("location", StringType, nullable = true)
//  ))
//
//  val reportingPlansSchema = StructType(Array(
//    StructField("plan_name", StringType, nullable = true),
//    StructField("plan_id_type", StringType, nullable = true),
//    StructField("plan_id", StringType, nullable = true),
//    StructField("plan_market_type", StringType, nullable = true)
//  ))
//
//  val reportingStructureSchema = StructType(Array(
//    StructField("reporting_plans", ArrayType(reportingPlansSchema), nullable = true),
//    StructField("in_network_files", ArrayType(fileLocationSchema), nullable = true),
//    StructField("allowed_amount_file", fileLocationSchema, nullable = true)
//  ))
//
//  val schema = StructType(Array(
//    StructField("reporting_entity_name", StringType, nullable = true),
//    StructField("reporting_entity_type", StringType, nullable = true),
//    StructField("version", StringType, nullable = true),
//    StructField("reporting_structure", ArrayType(reportingStructureSchema), nullable = true)
//  ))
//
//  // Load JSON file into DataFrame with multiLine and permissive mode options
//  val df = spark.read
//    .option("multiLine", true)
//    .option("mode", "PERMISSIVE")
//    .schema(schema)
//    .json("src/main/resources/health/2024-06-01_anthem_index.json")
//
//  // Show the initial DataFrame to confirm schema application
//  df.show(false)
//  df.printSchema()
//
//  // Explode reporting_structure to flatten the DataFrame
//  val explodedDF = df
//    .withColumn("reporting_structure", explode(col("reporting_structure")))
//    .withColumn("reporting_plans", col("reporting_structure.reporting_plans"))
//    .withColumn("in_network_files", col("reporting_structure.in_network_files"))
//    .withColumn("allowed_amount_file", col("reporting_structure.allowed_amount_file"))
//    .drop("reporting_structure")
//
//  // Show exploded DataFrame to inspect structure
//  explodedDF.show(false)
//  explodedDF.printSchema()
//
//  // Explode reporting_plans to further flatten the DataFrame
//  val flattenedPlansDF = explodedDF
//    .withColumn("reporting_plans", explode(col("reporting_plans")))
//    .select(
//      col("reporting_entity_name"),
//      col("reporting_entity_type"),
//      col("reporting_plans.plan_name"),
//      col("reporting_plans.plan_id_type"),
//      col("reporting_plans.plan_id"),
//      col("reporting_plans.plan_market_type"),
//      col("in_network_files"),
//      col("allowed_amount_file")
//    )
//
//  // Show flattened plans DataFrame to inspect structure
//  flattenedPlansDF.show(false)
//  flattenedPlansDF.printSchema()
//
//  // Flatten in_network_files
//  val flattenedInNetworkDF = flattenedPlansDF
//    .withColumn("in_network_files", explode_outer(col("in_network_files")))
//    .select(
//      col("reporting_entity_name"),
//      col("reporting_entity_type"),
//      col("plan_name"),
//      col("plan_id_type"),
//      col("plan_id"),
//      col("plan_market_type"),
//      col("in_network_files.description").alias("description"),
//      col("in_network_files.location").alias("location"),
//      col("allowed_amount_file.description").alias("allowed_amount_description"),
//      col("allowed_amount_file.location").alias("allowed_amount_location")
//    )
//
//  // Show flattened in-network files DataFrame to inspect structure
//  flattenedInNetworkDF.show(false)
//  flattenedInNetworkDF.printSchema()
//
//  // Filter rows where description contains 'PPO' and 'NEW YORK'
//  val filteredDF = flattenedInNetworkDF
//    .filter(col("description").isNotNull && col("description").contains("PPO") && col("description").contains("NEW YORK"))
//    .dropDuplicates("location")
//
//  // Show the resulting DataFrame
//  filteredDF.show(false)
//  filteredDF.printSchema()


  // println(df.count())

  // spark.stop()
}

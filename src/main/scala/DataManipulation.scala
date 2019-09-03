import org.apache.spark.sql.{RelationalGroupedDataset, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{
  IntegerType,
  StringType,
  StructField,
  StructType
}
import org.apache.spark.sql.functions._

object DataManipulation {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("Ridecell")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val inputSchema =
      StructType(
        StructField("dateValue", IntegerType, true) ::
          StructField("registrar", StringType, false) ::
          StructField("private_agency", StringType, false) ::
          StructField("state", StringType, false) ::
          StructField("district", StringType, false) ::
          StructField("sub_district", StringType, false) ::
          StructField("pincode", IntegerType, false) ::
          StructField("gender", StringType, false) ::
          StructField("age", IntegerType, false) ::
          StructField("aadhaar_generated", IntegerType, false) ::
          StructField("rejected", IntegerType, false) ::
          StructField("mobile_number", IntegerType, false) ::
          StructField("email_id", IntegerType, false) :: Nil)

    val filePath = "/Users/ankitagrahari/Downloads/aadhaar_data.csv"
    //Checkpoint 1
    //1.Load the data into Spark.
    val inputDF = spark.read
      .format("csv")
      .option("header", "false")
      .schema(inputSchema)
      .load(filePath)

    //2.View/result of the top 25 rows from each individual agency.
    val windowSpec = Window.partitionBy("private_agency").orderBy("pincode")
    val newViewDF =
      inputDF.withColumn("row_number_agency", row_number().over(windowSpec))
    val filtered_data = newViewDF.filter(r =>
      r.get(r.fieldIndex("row_number_agency")).asInstanceOf[Int] <= 25)

    val stateGroup = inputDF.groupBy("state")
    //Checkpoint 2
    //1.Describe the schema.
    inputDF.printSchema()

    //2.Find the count and names of registrars in the table.
    println("==count and names of registrars in the table==")
    inputDF.select("registrar").distinct().show()
    println(
      "count and names of registrars in the table ====> "
        + inputDF.select("registrar").distinct().count())
    //3.Find the number of states, districts in each state and sub-districts in each district.
    //Number of states
    val stateGroupedDf =
      inputDF.groupBy("state", "district", "sub_district").count()
    val numStates = stateGroupedDf.select("state").distinct().count()
    println("Number of States: " + numStates)
    //Num districts in each state
    val numDistInState =
      stateGroupedDf.groupBy("state").agg(countDistinct("district"))
    numDistInState.show()
    //Num Sub District in each district
    val numSubInDist =
      stateGroupedDf.groupBy("district").agg(countDistinct("sub_district"))
    numSubInDist.show()
    //4.Find out the names of private agencies for each state
    val privateAgencyInState =
      stateGroup.agg(collect_set("private_agency"))
    privateAgencyInState.show()

    //CHECKPOINT 3
    //1.Find top 3 states generating most number of Aadhaar cards?
    val aadhaarInEachState = stateGroup
      .agg(sum("aadhaar_generated").alias("count_aadhaar"))
    aadhaarInEachState.sort(desc("count_aadhaar")).show(3)
    //2.Find top 3 districts where enrolment numbers are maximum?
    inputDF
      .groupBy("district")
      .agg(sum("aadhaar_generated").alias("count_aadhaar"))
      .sort(desc("count_aadhaar"))
      .show(3)
    //3.Find the no. of Aadhaar cards generated in each state
    aadhaarInEachState.show()

    //CHECKPOINT 4
    //1.Find the number of unique pin codes in the data?
    val numPinCode = inputDF.select("pincode").distinct().count()
    println("Number of unique pin codes: " + numPinCode)
    //2.Find the number of Aadhaar registrations rejected in Uttar Pradesh and Maharashtra
    stateGroup
      .agg(sum("rejected").alias("sum_rejected"))
      .filter(
        r =>
          r.get(r.fieldIndex("state")).toString == "Maharashtra" || r
            .get(r.fieldIndex("state"))
            .toString == "Uttar Pradesh")
      .show()

    //CHECKPOINT 5
    //1.Find the top 3 states where the percentage of Aadhaar cards being generated for males is the highest.
    inputDF.select("gender").distinct().show()
    val top3MaleState = inputDF
      .filter(r => r.get(r.fieldIndex("gender")) == "M")
      .groupBy("state")
      .agg(sum("aadhaar_generated").alias("count_aadhaar"))
      .sort(desc("count_aadhaar"))
      .select("state")

    top3MaleState.show(3)
    //2.Find in each of these 3 states, identify the top 3 districts where the percentage of Aadhaar cards being rejected for females is the highest.
    val stateList = top3MaleState.take(3).map(r => r.get(0).toString).toList
    val windowSpecForState = Window.partitionBy("state").orderBy(desc("aggSum"))
    val rawData = inputDF
      .filter(
        r =>
          r.get(r.fieldIndex("gender")) == "F")
      .filter(r => stateList.contains(r.get(r.fieldIndex("state"))))
      .groupBy("state", "district")
      .agg(sum("rejected").alias("aggSum"))

    rawData
      .withColumn("row_number_agency", rank().over(windowSpecForState))
      .filter(r => r.get(r.fieldIndex("row_number_agency")).asInstanceOf[Int] <= 3).show()

    //3.Find the summary of the acceptance percentage of all the Aadhaar cards applications by bucketing the age group into 10 buckets.
  }
}

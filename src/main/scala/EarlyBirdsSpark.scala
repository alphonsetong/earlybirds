import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object EarlyBirdsSpark {

  val ratingMin : Float = 0.01F
  val malusMultiplier : Float = 0.95F

  val destFolder = "./src/main/resources/"
  val sourceFile = "./src/main/resources/xag-mini.csv"

  def main(args: Array[String]): Unit = {

    implicit val sparkSession = SparkSession.builder
        .appName("EarlyBirds")
        .master("local[*]")
        .getOrCreate

    val sqlContext = sparkSession.sqlContext

    val xagSchema = StructType(Array(
      StructField("userId", StringType, true),
      StructField("itemId", StringType, true),
      StructField("rating", FloatType, true),
      StructField("timestampLong", LongType, true)))

    val initialDF : DataFrame = sqlContext.read
      .format("com.databricks.spark.csv")
      .schema(xagSchema)
      .load(sourceFile)


    val userDF : DataFrame = dfUserIdString(initialDF).persist
    val itemDF : DataFrame = dfItemIdString(initialDF).persist

    val maxTimeStampLong : Long= getMaxTimestamp(initialDF)


    val correctedRatingFunc : ((Long,Float) => Float) = (timestampLong : Long, rating : Float )=> {
      scala.math.pow(
        malusMultiplier,
        nbDaysFromTimestampLong(maxTimeStampLong - timestampLong)
      ).toFloat * rating
    }
    val correctedRatingFuncSQL = udf(correctedRatingFunc)


    val aggratingDF : DataFrame = initialDF
      .withColumn("ratingCorrected",correctedRatingFuncSQL(col("timestampLong"),col("rating")))
      .withColumn("nbDaysDiffFromMax",col("ratingCorrected") - col("timestampLong"))
      .filter(col("ratingCorrected") > lit(ratingMin))

      .join(userDF,Seq("userId"),"left_outer")
      .join(itemDF,Seq("itemId"),"left_outer")

      .select("userIdString","itemIdString","ratingCorrected")
      .groupBy("userIdString","itemIdString").agg(sum("ratingCorrected").as("sumRating"))


   // println(aggratingDF.show())

    storeDataInCSV(aggratingDF,destFolder,"aggratings.csv")
    storeDataInCSV(userDF,destFolder,"lookupuser.csv")
    storeDataInCSV(itemDF,destFolder,"lookup_product.csv")

    sparkSession.stop
  }

  /**
    * convert timestamplong in number of days
    * @param l
    * @return
    */
  def nbDaysFromTimestampLong(l : Long): Long ={
    val oneDayInLong : Long = 24*60*60*1000
    l/oneDayInLong
  }

  /**
    * get the max Timestamp from the initial Dataframe
    * @param df
    * @param sparkSession
    * @return
    */
  def getMaxTimestamp(df: DataFrame)(implicit  sparkSession: SparkSession): Long = {
    df.select(max("timestampLong")).head.getLong(0)
  }

  /**
    * build the Dataframe (userId,userIdString) from the initial Dataframe
    * @param df
    * @param sparkSession
    * @return
    */
  def dfUserIdString(df: DataFrame)(implicit  sparkSession: SparkSession) : DataFrame = {
     df.select("userId")
      .distinct
      .select(
        col("userId"),
        (rank().over(Window.orderBy("userId"))-1).as("userIdString")
      )
  }

  /**
    * build the Dataframe (itemId,itemIdString) from the initial Dataframe
    * @param df
    * @param sparkSession
    * @return
    */
  def dfItemIdString(df: DataFrame)(implicit  sparkSession: SparkSession) : DataFrame = {
    df.select("itemId")
      .distinct
      .select(
        col("itemId"),
        (rank().over(Window.orderBy("itemId"))-1).as("itemIdString")
      )
  }

  /**
    * save df in csv in outputPat/filename
    * @param df
    * @param outputPath
    * @param filename
    */
  def storeDataInCSV(df: DataFrame,outputPath : String, filename : String): Unit ={
    val tmpDir = "tmp"
    val sourceTmp = outputPath+tmpDir

    df.write
      .format("com.databricks.spark.csv")
      .save(sourceTmp)

    merge(sourceTmp,outputPath+filename)
  }


  /**
    * merge all file from srcPath in dstPath
    * @param srcPath
    * @param dstPath
    */
  def merge(srcPath: String, dstPath: String): Unit =  {
    val hadoopConfig = new Configuration()
    val hdfs = FileSystem.get(hadoopConfig)
    FileUtil.copyMerge(hdfs, new Path(srcPath), hdfs, new Path(dstPath), true, hadoopConfig, null)
  }


}

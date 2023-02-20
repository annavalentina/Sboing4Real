import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.io.Source
import java.util.Calendar
import java.text.SimpleDateFormat
import org.apache.spark.sql.functions.to_timestamp
import org.apache.spark.sql.functions._
import java.util.TimeZone
object Main {
  def main(args:Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val spark: SparkSession = SparkSession.builder()
      .appName("FreeFlow")
      .getOrCreate()

    //Read arguments
    val days=args(0).toInt
    val druidDataSource=args(1)
    val zkHosts=args(2).toString()
    val postgresUsername=args(3)
    val postgresPassword=args(4)
    val postgresUrl=args(5)
    val postgresTable=args(6)

    //Find start date
    val timezone=TimeZone.getTimeZone("Europe/Athens")
    val date=new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    val now=Calendar.getInstance(timezone)
    now.add(Calendar.DAY_OF_YEAR,-days)
    now.set(Calendar.HOUR_OF_DAY,0)
    now.set(Calendar.MINUTE,0)
    now.set(Calendar.SECOND,0)
    val dateToUse="'"+date.format(now.getTime())+"'"//The start date
    val currentDate=date.format(Calendar.getInstance(timezone).getTime())//Date to use for the timestamp

    //Connect to Druid
    val df = spark.read.format("org.rzlabs.druid").
      option ("druidDatasource", druidDataSource).
      option ("zkHost",zkHosts)
      .option("zkSessionTimeout",3000000).load
    df.createOrReplaceTempView("ds")

    //Postgresql properties
    val prop = new java.util.Properties
    prop.setProperty("driver", "org.postgresql.Driver")
    prop.setProperty("user", postgresUsername)
    prop.setProperty("password", postgresPassword)


    import  spark.implicits._
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    import org.apache.spark.sql.expressions.Window
    import org.apache.spark.sql.functions.row_number

//Get all entries from Druid
    val druidAllEntries = spark.sql(s"SELECT mean_speed AS speed,segment_id as segmentid,window_start as windowstart FROM ds" +
      s" WHERE window_start >= ($dateToUse)").cache()
druidAllEntries.count()

    //Group by segment id and window_start
    val druidEntriesGrouped=druidAllEntries.groupBy("segmentid","windowstart").agg(avg("speed").alias("speed"))
      .withColumn("id",row_number().over(Window.partitionBy("segmentid").orderBy($"speed".desc)))
      .drop("windowstart").cache()
    //If there exist entries
if(druidEntriesGrouped.head(1).isEmpty==false) {

  //Find how many entries are in the 5% faster 5-mins of each segment
  val top5 = druidEntriesGrouped.groupBy("segmentid").agg(count("speed").alias("count")).withColumn("top5", (ceil($"count" * 0.05)))
    .withColumnRenamed("segmentid", "segmentid2").drop("count")

  //Limit to top 5% faster 5-mins for each segment
  val druidEntriesLimited = druidEntriesGrouped.join(top5, druidEntriesGrouped("segmentid") === top5("segmentid2"), "left").drop("segmentid2")
    .filter($"id" <= $"top5").drop("id", "top5")
  druidEntriesLimited.createOrReplaceTempView("df")

  //Write to postgresql the median of speedss
 val medianSpeeds=spark.sql("select segmentid, percentile_approx(speed, 0.5) as speed from df group by segmentid ").withColumn("date", to_timestamp(lit(currentDate)))
   .write.mode("append").jdbc(postgresUrl, postgresTable, prop)
}
    druidEntriesGrouped.unpersist()
    druidAllEntries.unpersist()
  }
}

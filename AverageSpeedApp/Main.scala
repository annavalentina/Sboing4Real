import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level
import java.util.Calendar
import java.util.TimeZone
import java.text.SimpleDateFormat
import org.apache.spark.sql.functions.to_timestamp
import org.apache.spark.sql.functions._


object Main {
  def main(args:Array[String]): Unit = {


    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val spark: SparkSession = SparkSession.builder()
      .appName("AverageSpeeds")
      .getOrCreate()

    //Read arguments
    val days=args(0).toInt
    val druidDataSource=args(1)
    val zkHosts=args(2).toString()
    val postgresUsername=args(3)
    val postgresPassword=args(4)
    val postgresUrl=args(5)
    val postgresTable=args(6)

    val timezone=TimeZone.getTimeZone("Europe/Athens")
    val date=new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    val now=Calendar.getInstance(timezone)
    now.add(Calendar.DAY_OF_YEAR,-days)
    now.set(Calendar.HOUR_OF_DAY,0)
    now.set(Calendar.MINUTE,0)
    now.set(Calendar.SECOND,0)
    val dateToUse="'"+date.format(now.getTime())+"'"//The start date
    val currentDate=date.format(Calendar.getInstance(timezone).getTime())//Date to use for the timestamp
	
	val currentDayOfTheWeek=Calendar.getInstance(timezone).get(Calendar.DAY_OF_WEEK)
	val dayOfTheWeek={
    if(currentDayOfTheWeek==1){7}
	else{currentDayOfTheWeek-1}}
    val dayOfTheWeekS="'"+(dayOfTheWeek).toString+"'"

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

    //Get entries from druid
    val druidAllEntries = spark.sql(s"SELECT segment_id AS segmentid, mean_speed AS speed,dayofweek(window_start) as  day," +
      s" hour(window_start) as hour,iot_count as iotCount FROM ds WHERE  window_start>= ($dateToUse)").cache().where('day=== dayOfTheWeek).cache()
  druidAllEntries.count()

    //Group entries by segment id and hour
    val druidEntriesGrouped=druidAllEntries.groupBy($"segmentid",$"hour")
    .agg(sum($"speed"*$"iotCount").as("sum"),
      sum("iotCount").as("count"))
    .withColumn("wavg",$"sum"/$"count")
    .withColumn("day", lit(dayOfTheWeek))
    .withColumn("date", to_timestamp(lit(currentDate))).cache()

    //If there exist data
        if (druidEntriesGrouped.head(1).isEmpty == false) {

          //Get entries from Postgresql for the same day
          val postgresEntries = spark.read.jdbc(postgresUrl, s"(select segmentid,hour,count,sum, wavg from $postgresTable where day=$dayOfTheWeekS) as a", prop)
            .withColumnRenamed("hour", "hour2").withColumnRenamed("count", "count2")
            .withColumnRenamed("segmentid", "segmentid2")
            .withColumnRenamed("sum", "sum2").withColumnRenamed("wavg", "wavg2")

          //If there exist entries in Postgresql
          if (postgresEntries.head(1).isEmpty == false) {

            //Join Postgresql-Druid data
            val joinedEntries = druidEntriesGrouped.as("druidEntriesGrouped").join(postgresEntries.as("postgresEntries"),
              $"druidEntriesGrouped.segmentid"===$"postgresEntries.segmentid2" && $"druidEntriesGrouped.hour" === $"postgresEntries.hour2", "left")
              .na.fill(0)
              .select($"segmentid", $"day", $"hour", ($"count" + $"count2").alias("count"), ($"sum" + $"sum2").alias("sum"),
                (when($"wavg2"===0,$"wavg").otherwise($"wavg" * 0.2 + $"wavg2" * 0.8)).alias("wavg"), (($"sum" + $"sum2") / ($"count" + $"count2")).alias("avg"), $"date")
            joinedEntries.write.mode("append").jdbc(postgresUrl, postgresTable, prop) //Write to Postgresql
          }
          else {
            druidEntriesGrouped.withColumn("avg", ($"wavg")).write.mode("append").jdbc(postgresUrl, postgresTable, prop) //Write to Postgresql
          }
          druidEntriesGrouped.unpersist()
        }

    druidAllEntries.unpersist()

    }


}

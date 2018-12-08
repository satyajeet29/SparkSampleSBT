package pradhan.com.spark

/**
  * Created by satyajeetpradhan on 12/4/18.
  */
// scalastyle:off println
import org.apache.spark.sql.SaveMode
// $example on:init_session$
import org.apache.spark.sql.SparkSession
// $example off:init_session$

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

// One method for defining the schema of an RDD is to make a case class with the desired column
// names and types.
//case class Record(key: Int, value: String)
case class LogRecord( host: String, timeStamp: String, url:String,httpCode:Int)

object SparkSQLSample {
  def main(args: Array[String]) {
    // $example on:init_session$

    //Pattern matching style
    val PATTERN = """^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+)(.*)" (\d{3}) (\S+)""".r

    //Parsing log record function
    def parseLogLine(log: String): LogRecord = {
      val res = PATTERN.findFirstMatchIn(log)
      if (res.isEmpty)
      {
        //println("Rejected Log Line: " + log)
        LogRecord("Empty", "", "",  -1 )
      }
      else
      {
        val m = res.get
        LogRecord(m.group(1), m.group(4),m.group(6), m.group(8).toInt)
      }
    }

    /*
			Declaring a spark session
    */
    val spark = SparkSession
      .builder
      .appName("SparkSQLSample")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    // Importing the SparkSession gives access to all the SQL functions and implicit conversions.
    import spark.implicits._
    // $example off:init_session$

    val conf = new SparkConf()
      .setAppName("SparkSQLSample")
      .set("spark.driver.allowMultipleContexts", "true")
      .setMaster("local")
      .set("spark.hadoop.validateOutputSpecs", "false")
    //the above line is intended to overwrite the existing files

    val sc = new SparkContext(conf)

    //Read the text file using spark context
    val logFile = sc.textFile("/data/spark/project/NASA_access_log_Aug95.gz")

    val accessLog = logFile.map(parseLogLine)
    val accessDf = accessLog.toDF()
    //accessDf.printSchema
    accessDf.createOrReplaceTempView("nasalog")
    val output = spark.sql("select * from nasalog")
    output.createOrReplaceTempView("nasa_log")
    //println(output.count())
    spark.sql("cache TABLE nasa_log")


    //1:
    val assmt1 = spark.sql("select url,count(*) as req_cnt from nasa_log where upper(url) like '%HTML%' group by url order by req_cnt desc LIMIT 10")
    //println(assmt1.count())
    //println("Assignment #1:")
    //assmt1.rdd.map(row => s"Url: ${row(0)}, Count: ${row(1)}").collect().foreach(println)
    assmt1.rdd.map(row => s"${row(0)}  ${row(1)}").saveAsTextFile("SparkProjectSQLsbt/Problem1")

    //2:
    val assmt2 = spark.sql("select host,count(*) as req_cnt from nasa_log group by host order by req_cnt desc LIMIT 20")
    //println(assmt2.count())
    //println("Assignment #2:")
    //assmt2.rdd.map(row => s"Host: ${row(0)}, Count: ${row(1)}").collect().foreach(println)
    assmt2.rdd.map(row => s"${row(0)}  ${row(1)}").saveAsTextFile("SparkProjectSQLsbt/Problem2")

    //3:
    val assmt3 = spark.sql("select substr(timeStamp,1,14) as timeFrame,count(*) as req_cnt from nasa_log where substr(timeStamp,1,14) not like '' group by substr(timeStamp,1,14) order by req_cnt desc LIMIT 5")
    //println(assmt3.count())
    //println("Assignment #3: descending")
    //assmt3.rdd.map(row => s"Time-frame: ${row(0)}, Count: ${row(1)}").collect().foreach(println)
    assmt3.rdd.map(row => s"${row(0)}  ${row(1)}").saveAsTextFile("SparkProjectSQLsbt/Problem3")

    //4:
    val assmt4 = spark.sql("select substr(timeStamp,1,14) as timeFrame,count(*) as req_cnt from nasa_log  where substr(timeStamp,1,14) not like ''  group by substr(timeStamp,1,14) order by req_cnt  LIMIT 5")
    //println(assmt4.count())
    //println("Assignment #4: ascending")
    //assmt4.rdd.map(row => s"Time-frame: ${row(0)}, Count: ${row(1)}").collect().foreach(println)
    assmt4.rdd.map(row => s"${row(0)}  ${row(1)}").saveAsTextFile("SparkProjectSQLsbt/Problem4")

    //5:
    //Obtaining distinct count of httpCode
    val assmt5 = spark.sql("select httpCode,count(*) as req_cnt from nasa_log where httpCode not like '-1' group by httpCode ORDER BY req_cnt desc")
    //println(assmt5.count())
    //println("Assignment #5:")
    //assmt5.rdd.map(row => s"HTTPCode: ${row(0)}, Count: ${row(1)}").collect().foreach(println)
    assmt5.rdd.repartition(1).map(row => s"${row(0)} - ${row(1)}").saveAsTextFile("SparkProjectSQLsbt/Problem5") // keyword repartition(1) prevents building multiple files

    spark.stop()
  }
}

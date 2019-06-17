import org.apache.spark.sql.{Encoders, Row, SparkSession, functions}
import org.apache.spark.SparkConf

object Main {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("mapper")

    val session = SparkSession.builder().config(conf).appName("mapper").master("local[*]").getOrCreate()
    import session.implicits._

    val jsonRDD = session.sparkContext.parallelize(Seq(
      """
         { "playlist":
         [
          {
            "endTime" : "2018-12-24 02:44",
            "artistName" : "Florence + The Machine",
            "trackName" : "Drumming Song - MTV Unplugged, 2012",
            "msPlayed" : 181417
          },
          {
            "endTime" : "2018-12-24 02:47",
            "artistName" : "James Arthur",
            "trackName" : "Say You Won't Let Go",
            "msPlayed" : 187802
          },
          {
            "endTime" : "2018-12-24 02:51",
            "artistName" : "Haley Klinkhammer",
            "trackName" : "Can't Help Falling In Love",
            "msPlayed" : 197648
          }
        ],
        "music": 2
      }"""
        .stripMargin))
    val frame = session.sqlContext.read.json(jsonRDD)


//    frame.printSchema()
    val col = frame.select(functions.explode($"playlist")).toDF()
    val upper = col.select('col.getItem("artistName") as 'artistName)
//        .as(Encoders.STRING)
//        .map(s => s.toUpperCase)

//    upper.printSchema()
    upper.columns.foreach(s => println(s))
//    upper.select("artistName")
//    upper.collect().foreach(r => println(r))


  }

}

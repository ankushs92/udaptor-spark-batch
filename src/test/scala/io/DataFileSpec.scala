package io

import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DataFileSpec extends org.specs2.mutable.Specification {

  "DataFile" >> {



    "Register simple map and transform" >> {

      val session = SparkSession.builder().appName("mapper").master("local[1]").getOrCreate()
      val json = """{"1": "a"}"""

      val file = new DataFile(json, session)

      file.registerMap("file_jsonöø1", "another_file_jsonöøa")
//      val value = file.performMaps()
      file.performMapsAndReturnFile()
//      println(value.collect())
//
      true should beTrue

    }

    "Register simple nested map and transform" >> {
      val session = SparkSession.builder().appName("mapper").master("local[1]").getOrCreate()
      val json = """{"1": {"a" :"b"}}"""

      val file = new DataFile(json, session)

      file.registerMap("file_jsonöø_1øa", "another_file_jsonöøa")

      file.performMapsAndReturnFile()

      true should beTrue

    }

    "Register deeper nested map and transform" >> {
      val session = SparkSession.builder().appName("mapper").master("local[1]").getOrCreate()
      val json = """{"1": {"a" : {"b": "c"}}}"""

      val file = new DataFile(json, session)

      file.registerMap("file_jsonöø_1ø_aøb", "another_file_jsonöøz")

      file.performMapsAndReturnFile()

      true should beTrue

    }

    "Register complex map and transform" >> {

      val session = SparkSession.builder().appName("mapper").master("local[1]").getOrCreate()
      val json = """
         { "playlist":
         [
          {
            "endTime" : "2018-12-24 02:44",
            "artistName" : "Florence + The Machine",
            "trackName" : "Drumming Song - MTV Unplugged, 2012"

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
        ]
      }"""

      val file = new DataFile(json, session)

      file.registerMap("file_jsonöø_playlistπøartistName", "another_file_jsonöø_playlistπønameOfTheArtist")
      file.registerMap("file_jsonöø_playlistπømsPlayed", "another_file_jsonöø_playlistπøtimePlayed")

      val value = file.performMapsAndReturnFile()

//      println(value.collect())

      true should beTrue

    }


  }


}

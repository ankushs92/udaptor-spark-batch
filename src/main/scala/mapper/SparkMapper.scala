package mapper

import io.DataFile
import org.apache.spark.{SparkConf, SparkContext}

class SparkMapper (tuples: List[Tuple2[String, String]], inputFile: DataFile, outputFile: DataFile) {

    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("mapper")

    val sc = new SparkContext(conf)

    def map(inputFormat: String, outputFormat: String): Unit = {

    }
}

import org.apache.spark.SparkContext
import SparkContext._
import java.lang.Number
import java.lang.Integer

object shoppers {

  private def parseLineToIntIntIntInt(line: String): (Int, (Int, Int, Int)) = {
    val splitted = line.split(" ")
    (splitted(0).toInt, (splitted(1).toInt, splitted(2).toInt, splitted(3).toInt))
  }

  def main(args:Array[String]) {
    val shoplogs_dir = args(0)
    val output_dir = args(1)
    val paralelism_string= args(2)
    val paralelism = paralelism_string.toInt
    System.setProperty("spark.default.parallelism", paralelism_string)
    System.setProperty("spark.worker.timeout", "60000")
    System.setProperty("spark.akka.timeout", "60000")
    System.setProperty("spark.storage.blockManagerHeartBeatMs", "60000")
    System.setProperty("spark.akka.storage.retry.wait", "60000")
    val sc = new SparkContext("spark://10.11.12.61:7077", "main_spark",
      "/home/icg27/spark-0.9.0-incubating/",
      Seq("/home/icg27/Musketeer/tests/spark_shoppers/target/scala-2.10/shoppers_2.10-1.0.jar"))

    val shoplogs_input = sc.textFile("hdfs://10.11.12.61:8020" + shoplogs_dir)

    val shoplogs:org.apache.spark.rdd.RDD[(Int, (Int, Int, Int))] =
      shoplogs_input.map(parseLineToIntIntIntInt)

    // Filter the users from the US.
    val shoplogs_filtered:org.apache.spark.rdd.RDD[(Int, (Int, Int, Int))] = shoplogs.filter(
      (input:(Int, (Int, Int, Int))) => input._2._1 == 1)

    // Get (user_id, price)
    val us_shoplogs:org.apache.spark.rdd.RDD[(Int, Int)] = shoplogs_filtered.map(
      (input:(Int, (Int, Int, Int))) => (input._1, input._2._3))

    // Get amount of money spent by each user (user_id, total)
    val spenders:org.apache.spark.rdd.RDD[(Int, Int)] = us_shoplogs.reduceByKey(_+_)

    // Filter users that have spent more than x
    val big_spenders:org.apache.spark.rdd.RDD[(Int, Int)] = spenders.filter(
      (input:(Int, Int)) => input._2 > 12000)

    big_spenders.saveAsTextFile("hdfs://10.11.12.61:8020" + output_dir)
    System.exit(0)
  }

}
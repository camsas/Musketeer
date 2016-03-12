import org.apache.spark.SparkContext
import SparkContext._
import java.lang.Number
import java.lang.Integer

object tpch {

  private def parsePart(line:String): (Int, (String, String)) = {
    val splitted = line.split(" ")
    (splitted(0).toInt, (splitted(3), splitted(6)))
  }

  private def parseLineitem(line:String): (Int, (Int, Double)) = {
    val splitted = line.split(" ")
    (splitted(1).toInt, (splitted(4).toInt, splitted(5).toDouble))
  }

  def main(args:Array[String]) {
    val lineitem_dir = args(0)
    val part_dir = args(1)
    val output_dir = args(2)
    val paralelism_string= args(3)
    val paralelism = paralelism_string.toInt
    System.setProperty("spark.default.parallelism", paralelism_string)
    System.setProperty("spark.worker.timeout", "60000")
    System.setProperty("spark.akka.timeout", "60000")
    System.setProperty("spark.storage.blockManagerHeartBeatMs", "60000")
    System.setProperty("spark.akka.storage.retry.wait", "60000")
    val sc = new SparkContext("spark://freestyle:7077", "main_spark",
      "/home/icg27/spark-0.9.0-incubating/",
      Seq("/home/icg27/Musketeer/tests/spark_tpch/target/scala-2.10/tpch_2.10-1.0.jar"))

    val input_parts = sc.textFile("hdfs://10.11.12.61:8020" + part_dir).map(parsePart)
    val input_lineitems = sc.textFile("hdfs://10.11.12.61:8020" + lineitem_dir).map(parseLineitem).cache()

    val cnt_lineitem:org.apache.spark.rdd.RDD[(Int, (Int, Int))] =
      input_lineitems.map({
        case (first, (second, third)) =>
          (first, (second, 1))
        case _ =>
          (0, (0, 0))
        })

    val avg_lineitem:org.apache.spark.rdd.RDD[(Int, Double)] = cnt_lineitem.reduceByKey(
      (r1:(Int, Int), r2:(Int, Int)) => (r1._1 + r2._1, r1._2 + r2._2)).map({
            case (first, (second, third)) =>
              (first, second / third * 0.2)
            case (0, (0, 0)) =>
              (0, 0)
            })

    // val agg_lineitem:org.apache.spark.rdd.RDD[(Int, Int)] =
    //   input_lineitems.map({
    //     case (first, (second, third)) =>
    //       (first, second)
    //     case _ =>
    //       (0, 0)
    //     }).reduceByKey(_+_)

    // val avg_lineitem:org.apache.spark.rdd.RDD[(Int, Double)] = agg_lineitem.join(cnt_lineitem, paralelism).map({
    //    case (first, (second, third)) =>
    //       (first, second / third * 0.2)
    //    case (0, (0, 0)) =>
    //       (0, 0)
    //    })

    // input_lineitems (Int, (Int, Double))
    // input_parts (Int, (String, String))
    // avg_lineitem (Int, Double)
    // linepart (Int, ((Int, Double), (String, String)))
    // lineavg (Int, (((Int, Double), (String, String)), Double))
    val linepart = input_lineitems.join(input_parts, paralelism).join(avg_lineitem, paralelism).filter((input:(Int, (((Int, Double),(String,String)),Double))) => input._2._1._2._1.equals("Brand#13") && input._2._1._2._2.equals("MEDBAG") && input._2._1._1._1 > input._2._2).map((input:(Int, (((Int, Double),(String,String)),Double))) => input._2._1._1._2)

    linepart.saveAsTextFile("hdfs://10.11.12.61:8020" + output_dir)

    System.exit(0)
  }

}

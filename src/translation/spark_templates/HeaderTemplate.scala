import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import java.lang.Number

object {{CLASS_NAME}} {
 def main(args:Array[String]) {
  System.setProperty("spark.default.parallelism", "180")
  System.setProperty("spark.worker.timeout", "60000")
  System.setProperty("spark.akka.timeout", "60000")
  System.setProperty("spark.storage.blockManagerHeartBeatMs", "60000")
  System.setProperty("spark.akka.storage.retry.wait", "60000")
  System.setProperty("spark.akka.frameSize", "10000")
  val sc = new SparkContext( "{{SPARK_MASTER}}","main_spark","{{SPARK_DIR}}",Seq("{{BIN_NAME}}"));

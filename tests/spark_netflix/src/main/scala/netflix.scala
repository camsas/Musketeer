import org.apache.spark.SparkContext
import SparkContext._
import java.lang.Number
import java.lang.Integer

object netflix {

  private def parseLineToIntInt(line: String): (Int, Int) = {
    val splitted = line.split(" ")
    (splitted(0).toInt, splitted(1).toInt)
  }

  private def parseLineToIntIntInt(line: String): (Int, (Int, Int)) = {
    val splitted = line.split(" ")
    // (movie_id, (user_id, rating))
    (splitted(0).toInt, (splitted(1).toInt, splitted(2).toInt))
  }

  private def mulPairMat(input:(Int, ((Int, Int), (Int, Int)))): ((Int, Int) , Int) = {
    ((input._1, input._2._2._1), input._2._1._2 * input._2._2._2)
  }

  def main(args:Array[String]) {
    val ratings_dir = args(0)
    val movies_dir = args(1)
    val output_dir = args(2)
    val paralelism_string= args(3)
    val pre_year = args(4).toInt
    val paralelism = paralelism_string.toInt
    System.setProperty("spark.default.parallelism", paralelism_string)
    System.setProperty("spark.worker.timeout", "60000")
    System.setProperty("spark.akka.timeout", "60000")
    System.setProperty("spark.storage.blockManagerHeartBeatMs", "60000")
    System.setProperty("spark.akka.storage.retry.wait", "60000")
    val sc = new SparkContext("spark://freestyle:7077", "main_spark",
      "/home/icg27/spark-0.9.0-incubating/",
      Seq("/home/icg27/Musketeer/tests/spark_netflix/target/scala-2.10/netflix_2.10-1.0.jar"))

    val input_movies = sc.textFile("hdfs://10.11.12.61:8020" + movies_dir)
    val input_ratings = sc.textFile("hdfs://10.11.12.61:8020" + ratings_dir)

    // (movie_id (user_id, rating))
    val ratings:org.apache.spark.rdd.RDD[(Int, (Int, Int))] =
      input_ratings.map(parseLineToIntIntInt)
    // (movie_id, date)
    val movies_sel:org.apache.spark.rdd.RDD[(Int, Int)] =
      input_movies.map(parseLineToIntInt).filter(
        (input:(Int, Int)) => input._2 < pre_year)
    // preferences_join(movie_id, (date, (user_id, rating)))
    // preferences(movie_id, (user_id, rating))
    val preferences:org.apache.spark.rdd.RDD[(Int, (Int, Int))] =
      movies_sel.join(ratings, paralelism).map(
        (input:(Int, (Int, (Int, Int)))) => (input._1, input._2._2)).cache()
    // transpose(user_id, (movie_id, rating))
    val transpose:org.apache.spark.rdd.RDD[(Int, (Int, Int))] =
      preferences.map((input:(Int, (Int, Int))) => (input._2._1, (input._1, input._2._2)))
    // mat(user_id, ((movie_id, rating), (movie_id, rating)))
    val mat:org.apache.spark.rdd.RDD[(Int, ((Int, Int), (Int, Int)))] =
      transpose.join(transpose, paralelism)

    // matmult((movie_id, movie_id), rating^2)
     val matmult:org.apache.spark.rdd.RDD[((Int, Int), Int)] = mat.map({
       case (uid, ((m1, r1), (m2, r2))) => ((m1, m2), r1 * r2)
       case _ => ((0, 0), 0)})
    // int_result_reduce((user_id, movie_id), agg_rating)
    // int_result(movie_id, (user_id, agg_rating))
    val int_result:org.apache.spark.rdd.RDD[(Int, (Int, Int))] =
      matmult.reduceByKey(_+_).map((input:((Int, Int), Int)) => (input._1._2, (input._1._1, input._2)))

    // matfinal(movie_id, ((user_id, agg_rating), (user_id, rating)))
    val matfinal:org.apache.spark.rdd.RDD[(Int, ((Int, Int), (Int, Int)))] = int_result.join(preferences)
    // ((movie_id, user_id), rating^2)
    val matmultfinal:org.apache.spark.rdd.RDD[((Int, Int), Int)] = matfinal.map(mulPairMat)
    // predicted_tmp((movie_id, user_id), agg_rating)
    // predicted(user_id, (movie_id, agg_rating))
    val predicted:org.apache.spark.rdd.RDD[(Int, (Int, Int))] =
      matmultfinal.reduceByKey(_+_).map((input:((Int, Int), Int)) => (input._1._2, (input._1._1, input._2)))
    val prediction:org.apache.spark.rdd.RDD[(Int, Int)] =
      predicted.reduceByKey((left:(Int, Int), right:(Int, Int)) =>
        if (left._2 > right._2) {
          left
        } else {
          right
        }).map((input:(Int, (Int, Int))) => (input._1, input._2._2))
    prediction.saveAsTextFile("hdfs://10.11.12.61:8020" + output_dir)
    System.exit(0)
  }

}

import org.apache.spark
import org.apache.spark.sql._
import org.apache.spark.sql.Dataset

class Test {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    val df = spark.read.json("examples/src/main/resources/people.json")
  }
}

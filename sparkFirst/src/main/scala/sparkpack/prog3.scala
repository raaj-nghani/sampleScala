package sparkpack
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
object prog3 {
  def main(args:Array[String]):Unit={
    val conf = new SparkConf().setAppName("check").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val spark = SparkSession.builder().getOrCreate()
    val date = current_date()
    val df = spark.read.format("csv").option("header","true").load("file:///d:/data/abcde.txt")
    df.show()
    df.printSchema()
    val df2 = df.withColumn("date",lit(date))
    df2.show()
  }
  
}
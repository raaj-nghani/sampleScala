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
    val df = spark.read.format("json").option("multiline","true").load("file:///d:/data/random10.json")
    df.show()
    df.printSchema()
    val flatdf= df.withColumn("results", expr("explode(results)"))
    .select(
    col("nationality"),
    col("results.user.cell"),
    col("results.user.dob"),
    col("results.user.email"),
    col("results.user.gender"),
    col("results.user.location.*"),
    col("results.user.md5"),
    col("results.user.name.*"),
    col("results.user.password"),
    col("results.user.phone"),
    col("results.user.picture.*"),
    col("results.user.registered"),
    col("results.user.salt"),
    col("results.user.sha1"),
    col("results.user.sha256"),
    col("results.user.username"),
    col("seed"),
    col("version"))
    flatdf.printSchema()
    flatdf.show()
   val df2 = flatdf.withColumn("date",lit(date))
   df2.printSchema()
   df2.show()
   
  }
  
}
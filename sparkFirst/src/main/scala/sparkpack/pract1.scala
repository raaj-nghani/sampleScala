package sparkpack
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._


object pract1 {
	def main(args:Array[String]):Unit={
			val conf = new SparkConf().setAppName("test").setMaster("local[*]")
					val sc = new SparkContext(conf)
					sc.setLogLevel("ERROR")
					val spark = SparkSession.builder().getOrCreate()
val df = spark.read.format("csv").option("header","true").load("file:///d:/data/abcde.txt")
df.show()
val df1 = df.withColumn("value", regexp_replace(col("value"), "(" , " "))
.withColumn("value", regexp_replace(col("value"), ")", ""))
.withColumn("col1", split(col("value"),"").getItem(1))
.withColumn("col2", split(col("value"),"").getItem(0)).show()

	}

}
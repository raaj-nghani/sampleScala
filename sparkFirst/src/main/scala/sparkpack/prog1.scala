package sparkpack
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
object prog1 {
	def main(args:Array[String]):Unit={
			val conf = new SparkConf().setAppName("first program").setMaster("local[*]")
					val sc = new SparkContext(conf)
					sc.setLogLevel("ERROR")
					val data = sc.textFile("file:///d:/data/txns")
					data.foreach(println)
					val gymdata = data.filter(x => x.contains("Gymnastics"))
					gymdata.foreach(println)

	}
}
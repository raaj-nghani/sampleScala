/*Requirement
- you have file known as usdata.csv sitting in d:/data/
- read usdata.csv
- iterate each line and filter length > 200
- after filter flatmap with delimiter with comma
- concate each line pf flattend data with zeyo
- iterate each element and replace - (hyphon) with nothing
- write the result to a textFile*/
package sparkpack
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
object prog2 {
	def main(args:Array[String]):Unit={
			val conf = new SparkConf().setAppName("first program").setMaster("local[*]")
					val sc = new SparkContext(conf)
					sc.setLogLevel("ERROR")

					println("----- Started -----")
					val data = sc.textFile("file:///d:/data/usdata.txt")
					data.take(10).foreach(println)
					
					println("----- Length Data -----")
					val fildata = data.map(len => len.length()>200)
					fildata.take(10).foreach(println)
					
					println(" ----- Split Data ----- ")
					val split = fildata.flatMap(x => x.split(","))
					split.take(10).foreach(println)
					
	}
}
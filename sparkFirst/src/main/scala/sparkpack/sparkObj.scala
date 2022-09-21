package sparkpack
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
object sparkObj {
	def main(args:Array[String]):Unit={
			val conf = new SparkConf().setAppName("first").setMaster("local[*]")
					val sc = new SparkContext(conf)
					sc.setLogLevel("ERROR")
					val data = sc.textFile("file:///d:/data/newdata.txt")
					data.foreach(println)
					val flatlist = data.flatMap(x => x.split("~"))
					flatlist.foreach(println)
					val statelist = flatlist.map(x => x.contains("state"))
					statelist.foreach(println)
					val citylist = flatlist.map(x => x.contains("city"))
					citylist.foreach(println)
					//val finalstate = statelist.map(x => x.replace("state->",""))
					//val finalcity = citylist.map(x => x.replace("city->",""))
					println("----- final State List -----")
					//finalstate.foreach(println)
					println("----- final City List -----")
					//finalcity.foreach(println)
					

	}

}
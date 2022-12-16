package pack
  import org.apache.spark._
  import org.apache.spark.sql._
object obj extends App{
 val aTest2=List(("a","aa",1),("b","bb",2),("c","cc",3))     
 val aTest3=aTest2.map((elem => elem._1 :+ elem._3*elem._3))        
 val aTest5=aTest3.map(e2=>e2.toList)     
 val aTest6=aTest5.collect{case List(a, b) => a -> b }.toList
 println(aTest6) 
}

OUTPUT
List((a,1), (b,4), (c,9))

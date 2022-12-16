val lis - List((102,20000),(103,30000),(140,56999)).toDF("empid","empsal")
lis.show()
val lis1 = List((0,10000,"0-10000"),(10001,20000,"10001-20000),(20001,30000,"20001-30000")).toDF("low","high","Bandwidth")

val result = lis.join(lis1,lis("empsal")) > lis1("low") && lis("empsal") <= lis1("high"),"right").select("Bandwidth","empsal").withColumn("No. of emp", expr("case when empsal is null then 0 else 1 end")).drop("empsal")
result.show()



OUTPUT

cala> lis1.show()
+-----+-----+-----------+
|  low| high|  Bandwidth|
+-----+-----+-----------+
|    0|10000|    0-10000|
|10001|20000|10001-20000|
|20001|30000|20001-30000|
+-----+-----+-----------+

cala> lis1.show()
+-----+-----+-----------+
|  low| high|  Bandwidth|
+-----+-----+-----------+
|    0|10000|    0-10000|
|10001|20000|10001-20000|
|20001|30000|20001-30000|
+-----+-----+-----------+

scala> result.show()
+-----------+----------+                                                        
|  Bandwidth|No. of emp|
+-----------+----------+
|    0-10000|         0|
|10001-20000|         1|
|20001-30000|         1|
+-----------+----------+


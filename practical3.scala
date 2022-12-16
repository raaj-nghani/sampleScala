val data = spark.read.format("csv").option("header","true").option("iinferSchema","true").load("file:///home/raj/data/practice.csv")



scala> data.show()
+---+--------+-----+------+------+
| id|    name| dept|deptno|salary|
+---+--------+-----+------+------+
|  1|rajkumar|   it|  i001| 45000|
|  2|  mukesh|   it|  i001| 35000|
|  3|   kiran|house|  h001| 12000|
|  4|    ranu|house|  h001| 15000|
|  5|  himesh|   hr|  h002| 44000|
|  6|   reena|   hr|  h002| 43000|
|  7|   pagal|   hr|  h002| 21000|
|  8| ratnesh|   it|  i001| 25000|
|  9|   rohit|house|  h001| 20000|
+---+--------+-----+------+------+


scala> data.createOrReplaceTempView("table")

scala> val res1 = spark.sql("select t.dept, max(t.salary) as maxs from table t where t.salary < (select max(salary) from table t2 where t2.dept = t.dept) group by t.dept")
res1: org.apache.spark.sql.DataFrame = [dept: string, maxs: string]

scala> res1.show()
+-----+-----+                                                                   
| dept| maxs|
+-----+-----+
|house|15000|
|   hr|43000|
|   it|35000|
+-----+-----+




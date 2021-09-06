 import org.apache.spark._
 import org.apache.spark.sql._



object Country{
  def main (args : Array[String]) : Unit = {
     val conf = new SparkConf().setAppName("Country")
     val sc = new SparkContext(conf)
     val ssc = new SQLContext(sc);
     
     import ssc.implicits._;

      
     val webclicks = sc.textFile(args(0))
     val fields = webclicks.map(x => x.split(",")) 
     val multi_col = fields.map(x => (x(0),x(1),x(2),x(3),x(4),x(5)))
     val df = multi_col.toDF()
     val df1 = df.groupBy("_2").count()
     df1.show()

     

    }
}
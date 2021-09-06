 import org.apache.spark._
 import org.apache.spark.sql._

case class WebClicksClass(AccessDate:String,AccessTime:String,HostIP:String,CSMethod:String,UserIP:String,URL:String,TimeSpent:Int,Domain:String,DeviceType:String)

object DeviceTimespent {
  def main (args : Array[String]) : Unit = {
     val conf = new SparkConf().setAppName("DeviceTimeSpent")
     val sc = new SparkContext(conf)
     val ssc = new SQLContext(sc);
     
     import ssc.implicits._;

      
     val webclicks = sc.textFile("file:")
     val fields = webclicks.map(x => x.split("\t")) 
     val multi_col = fields.map(x => (x(0),x(1),x(2),x(3),x(4),x(5).split("\\&")(0).split("=")(1),x(6).toInt,x(7),x(8)))
     val webclickscased = multi_col.map{case(a,b,c,d,e,f,g,h,i) => WebClicksClass(a,b,c,d,e,f,g,h,i)}
     val webclicksdf = webclickscased.toDF()
     webclicksdf.show(5,false)
     webclicksdf.createOrReplaceTempView("WebclicksView")
     val androidTotalTime = ssc.sql("SELECT DeviceType, SUM(TimeSpent) FROM WebclicksView WHERE DeviceType='android' GROUP BY DeviceType")
     androidTotalTime.show()

    }
}
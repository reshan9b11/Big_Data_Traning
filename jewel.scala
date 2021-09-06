import org.apache.spark._
object ProductTypeJewellery{
   def main(args:Array[String]): Unit = {
      val conf = new SparkConf().setAppName("ProductTypeJewellery")
      val sc = new SparkContext(conf)
      val Webclicks = sc.textFile(args(0))

      val Split3 = Webclicks.map( x => x.split("\t"))
      val Split4 = Split3.map( x => ( x(4), x(5).split("\\&")(0).split("=")(1)))
      val filtering = Split4.filter( x =>x._2.equals("jewellery") )
      val newPair = filtering.map{case(a,b) => (a,1)}
      val output2 = newPair.reduceByKey( (a,b) => a+1)
      output2.collect().foreach(println)
  }

}

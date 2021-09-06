import org.apache.spark._
import org.apache.spark.streaming._

object UsersProduct {

	val myUpdateFunc=(values:Seq[Int], state:Option[Int]) => {
	val currentCount = values.foldLeft(0)(_+_)
	val previousCount = state.getOrElse(0)
	Some(currentCount+previousCount)

}
def main(args :Array[String]) :Unit ={

	val conf = new SparkConf().setAppName("UsersProduct")
	val sc = new SparkContext(conf)
	val ssc = new StreamingContext(sc, Seconds(5))
	ssc.checkpoint("/home/vagrant/SparkStream/Buffer")

 	val streamData = ssc.socketTextStream("192.168.56.70",9999)

 	val split1 = streamData.map(x => x.split("\t"))
	 val pairDstream1 = split1.map(x => (x(4), x(5).split("\\&")(0).split("=")(1)))
 	val pairDstream2 = pairDstream1.filter(x => x._2 == "clothing" )
 	val pairDstream3 = pairDstream2.map{case(a,b) => (a,1)}

 	val usersClothingSearchCount = pairDstream3.updateStateByKey[Int](myUpdateFunc)


 	usersClothingSearchCount.print()
 
	ssc.start()
 	ssc.awaitTermination()
}
}

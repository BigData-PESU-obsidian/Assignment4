
import org.apache.spark.sql.functions._
import scala.util.control._
import scala.math.{abs, max}
import org.apache.spark.sql.SparkSession
object SparkPageRank{

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("SparkPageRank")
      .getOrCreate()

    val lines = spark.read.textFile(args(0)).rdd

	val pairs = lines.map{ s => val parts = s.split("\\s+") ;(parts(0), parts(1))  } 
	val links = pairs.distinct().groupByKey().cache() 
    var ranks = links.mapValues(v => 1.0)
    var oldrank = links.mapValues(v=>0.0)
    var diff=oldrank.join(ranks).map{case(a,b)=>Math.abs(b._2-b._1)}
    var iters=0
	val loop = new Breaks
	loop.breakable{
  	do{   
  	    iters=iters+1;
		    val contribs = links.join(ranks).values.flatMap{case (urls, rank) =>
		        val size = urls.size;
      			urls.map(url => (url, rank / size))}
				
      	ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
     	var diff=oldrank.join(ranks).map{case(a,b)=>Math.abs(b._2-b._1)}
		println("The maximum difference is: "+ diff.max+ " Iteration Number: " + iters)
		oldrank=ranks

		if(diff.max < 0.1){
		   println("Final maximum difference : "+ diff.max)
    	   println("Iretations till convergence : "+iters)
		   loop.break
		}

	  }while(diff.max>0.1)
	}
    val output = ranks.collect()
    //output.foreach(tup => println(tup._1 + " has rank: " + tup._2 + "."))
    spark.stop()

	}

}

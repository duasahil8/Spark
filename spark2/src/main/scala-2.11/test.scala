/**
  * Created by sdua on 3/20/2017.
  */

import org.apache.spark.{SparkConf, SparkContext}
import com.typesafe.config.ConfigFactory

import java.util.Calendar
object test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("Datasets Test")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)
    println(sc)
    val fileRDD = sc.textFile("C:\\Users\\sdua\\Downloads\\2005.csv")
    val firstLine = fileRDD.first()
    //val filtered = fileRDD.filter(x => !(x.contains(firstLine)))
    //println(filtered.first())
    val filtered = fileRDD.filter(x => !(x.contains(firstLine))).filter( x => !x.contains(",NA"))
    case class Flight(year : Int ,month : Int,day : Int,week : Int,carrier : String,arrDelay : Int,depDelay : Int,origin : String,dest : String,cancelled : Int)
    def getWeek(year:Int, month:Int,day:Int): Int ={
      val cal = Calendar.getInstance()
      cal.set(year,month-1,day);
      return cal.get(Calendar.WEEK_OF_YEAR)
    }

    val dataRDD = filtered.map{line =>val cols = line.split(",")
      Flight(cols(0).toInt, cols(1).toInt, cols(2).toInt, getWeek(cols(0).toInt, cols(1).toInt, cols(2).toInt) , cols(8),cols(14).toInt, cols(15).toInt,cols(16),cols(17), cols(21).toInt)}
    val air = ConfigFactory.load("airline.conf").getString("airline")
    println(air)
    val origin = dataRDD.filter(line => (line.origin == air & line.depDelay > 0)).map( x => ((x.year,x.week, x.carrier) , x.depDelay))
    val dest = dataRDD.filter(line => (line.dest == air & line.arrDelay > 0)).map( x => ((x.year, x.week, x.carrier) , x.arrDelay))

    val totalDelay = origin.union(dest).reduceByKey((x,y) => (x+y)).sortBy(_._1).saveAsTextFile("C:\\Users\\sdua\\airline\\")



  }
}
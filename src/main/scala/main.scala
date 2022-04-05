
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

//csvReader
import java.io.File
import java.io.FileReader
import java.io.FileNotFoundException
import com.github.tototoshi.csv._


trait OutputFunctions {

  def printRed(str: String, newLine: Boolean = true) = {
    if (newLine) {
      Console.println("\u001b[31m" + str + "\u001b[0m")
    } else {
      Console.print("\u001b[31m" + str + "\u001b[0m")
    }
  }
  def printYellow(str: String, newLine: Boolean = true) = {
    if (newLine){
      Console.println("\u001b[33m" + str + "\u001b[0m")
    } else {
      Console.print("\u001b[33m" + str + "\u001b[0m")
    }
  }
  def printGreen(str: String, newLine: Boolean = true) = {
    if (newLine){
      Console.println("\u001b[32m" + str + "\u001b[0m")
    } else {
      Console.print("\u001b[33m" + str + "\u001b[0m")
    }
  }

}

object myspark extends OutputFunctions{

// Returns 2d List of CSV file.
  def readCSV(file: String, printResult: Boolean = false, printLimit: Int = 20): List[List[Any]] = {
    // import File 
    val reader = CSVReader.open(new File(file))

    //type cast each ()() to its appropriate datatype 
    val result = reader.all()

    reader.close()

    if (printResult) {
      // print the first 20 rows, or until the end of the file if less than 20 rows.
      val upperLimit = if (result.length < printLimit) result.length else printLimit
      result.take(upperLimit).foreach(println)  

      printYellow("...and " + (result.length - 20) + " more.")
    }

    if (result(1).length == result(0).length ) {
      "CSV file is valid."
    } else if (result(1).length == 0){
      throw new Exception("Row 1 is empty.  For column typecasting, there must be at least one row of data.")
    } else {
      throw new AssertionError("Preliminary check failed.  CSV file is invalid.")
    }

    //return
    result
  }

  //combines all rows into one big list
  def combineLists(listOfLists: List[List[Any]])={
    var rows = listOfLists(0)
    listOfLists.foreach(lst => {
      //skip first row 
      if (lst != listOfLists(0)) {
        //append each row to rdd 
        rows = rows ++ lst
      }
    })
    rows
  }

  def wordList(lst: List[Any])={
    lst.flatMap(x => x.toString.split(" ")).toList
    lst
  }






  



  def test(){


  val conf= new SparkConf().setAppName("test").setMaster("local")
  val sc =new SparkContext(conf)
  val spark=SparkSession.builder().config(conf).getOrCreate()
  sc.setLogLevel("ERROR")

  val lines = readCSV("/home/jonesgc/Documents/countries.csv")
  val combinedRows = combineLists(lines)
  val words = wordList(combinedRows)

  // turn words to a string array 
  var wordsArray = Array[String]()
  words.foreach(word => {
    wordsArray = wordsArray :+ word.toString
  })

  val rdd = sc.parallelize(wordsArray)
  val rdd1 = rdd.flatMap(x => x.split(","))




  //convert lines to a String arrray 
  //val strArr = lines.map(lst => lst.mkString(","))
  //val rdd =sc.parallelize(strArr)
  //val rdd1 = rdd.flatMap(x=>x.split(" "))
  import  spark.implicits._
  println("rdd class: "+rdd.getClass)
  val df = rdd1.toDF("word")
  println(df.getClass)
  df.createOrReplaceTempView("tempTable")
  val rslt=spark.sql("select word,COUNT(1) from tempTable GROUP BY word ")
  rslt.show(1000,false)


  }

  def run(){
    //TODO: Get this function to work
    // Create Spark Session/Shell (context)
    val conf = new SparkConf().setAppName("SparkSQL").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val spark = new SparkSession.Builder().config(conf).getOrCreate()
    // Create a SparkContext to initialize Spark
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    import sqlContext.implicits._
    import hiveContext.implicits._

    // Get data from csv
    var csvRows = readCSV("/home/jonesgc/Documents/countries.csv")

    val df = combineLists(csvRows)
    
    println("df class: "+df.getClass)


  }






}


object Main extends App{

  def test(){


  val conf= new SparkConf().setAppName("test").setMaster("local")
  val sc =new SparkContext(conf)
  val spark=SparkSession.builder().config(conf).getOrCreate()
  sc.setLogLevel("ERROR")
  val line1 = "live life enjoy detox"
  val line2="learn apply live motivate"
  val line3="life detox motivate live learn"
  val rdd =sc.parallelize(Array(line1,line2,line3))
  val rdd1 = rdd.flatMap(x=>x.split(" "))
  import  spark.implicits._
  println("rdd class: "+rdd.getClass)
  val df = rdd1.toDF("word")
  println(df.getClass)
  df.createOrReplaceTempView("tempTable")
  val rslt=spark.sql("select word,COUNT(1) from tempTable GROUP BY word ")
  rslt.show(1000,false)


  }

  println("Hello")
  myspark.test()
  //myspark.readCSV("/home/jonesgc/Documents/countries.csv", true)

  //test()

    
}

import org.apache.spark.sql.SparkSession
import spark.implicits._
import java.io.File
import scala.collection.mutable.ArrayBuffer
//import scala.Math
//import scala.M
// For implicit conversions like converting RDDs to DataFrames

object p2{

    def main(args: Array[String]){
        val point_path = "hdfs://localhost:9000/project-3/p2/P.csv"

        /*val conf = SparkConf().setAppName("Project3-2").setMaster("local")
        val sc = SparkContext.getOrCreate()*/
        val point = sc.textFile(point_path)
	//point.collect().take(20).foreach(println)
	



        val Cell_C = (s : String)=>{
            val line = s.split(',')
            val x = line(0).toInt
            val y = line(1).toInt
            val cell = Math.min((x/20) +1 , 500)  +  (Math.min((10000 - y)/20 , 499 ) * 500 )
           
            val result = ArrayBuffer[(Int, String)]()
            //Int : cell indes, String : cell neighber information

            if(cell == 1){
                result += ((cell,"1,0"))
                result += ((cell+1,"0,1"))
                result += ((cell+500,"0,1"))
                result += ((cell+501,"0,1"))
            } else if(cell == 500){
                result += ((cell,"1,0"))
                result += ((cell-1,"0,1"))
                result += ((cell+499,"0,1"))
                result += ((cell+500,"0,1"))
            }else if(cell == 249501){
                result += ((cell,"1,0"))
                result += ((cell+1,"0,1"))
                result += ((cell-500,"0,1"))
                result += ((cell-499,"0,1"))
            }else if(cell == 250000){
                result += ((cell,"1,0"))
                result += ((cell-1,"0,1"))
                result += ((cell-500,"0,1"))
                result += ((cell-501,"0,1"))
            }else if(1<cell&&cell<500){//top
                result += ((cell,"1,0"))
                result += ((cell - 1,"0,1"))
                result += ((cell + 1,"0,1"))
                result += ((cell + 499,"0,1"))
                result += ((cell + 500,"0,1"))
                result += ((cell + 501,"0,1"))
            }else if(249500<cell&&cell<250000){//bottom
                result += ((cell,"1,0"))
                result += ((cell - 1,"0,1"))
                result += ((cell + 1,"0,1"))
                result += ((cell - 499,"0,1"))
                result += ((cell - 500,"0,1"))
                result += ((cell - 501,"0,1"))
            }else if(cell%500 == 1){//left
                result += ((cell,"1,0"))
                result += ((cell - 500,"0,1"))
                result += ((cell - 499,"0,1"))
                result += ((cell + 1,"0,1"))
                result += ((cell + 500,"0,1"))
                result += ((cell + 501,"0,1"))
            }else if(cell%500 == 0){//right
                result += ((cell,"1,0"))
                result += ((cell - 500,"0,1"))
                result += ((cell - 501,"0,1"))
                result += ((cell - 1,"0,1"))
                result += ((cell + 500,"0,1"))
                result += ((cell + 499,"0,1"))
            }else{//normal
                result += ((cell,"1,0"))
                result += ((cell - 1,"0,1"))
                result += ((cell + 1,"0,1"))
                result += ((cell - 499,"0,1"))
                result += ((cell - 500,"0,1"))
                result += ((cell - 501,"0,1"))
                result += ((cell + 499,"0,1"))
                result += ((cell + 500,"0,1"))
                result += ((cell + 501,"0,1"))
            }
            result.toArray
        }
	
	//val Coor = point.flatMap{line=>Cell_C(line)}
	//Coor.collect().foreach(println)

        val SumCell = (s1 : String, s2 : String)=>{
            val line1 = s1.split(",")
            val line2 = s2.split(",")
            val self_count = line1(0).toInt + line2(0).toInt
            val neighber_count = line1(1).toInt + line2(1).toInt
            val result = self_count.toString + "," + neighber_count.toString
            result
        }

        val Coor = point.flatMap{line=>Cell_C(line)}
        val Sum_Cell = Coor.reduceByKey((a,b)=>SumCell(a,b))
	//Sum_Cell.collect().foreach(println)
        val Cell_density =(s: (Int, String)) => {
            val cell = s._1
	    val temp = s._2
            val D_Info = temp.split(",")
            var neighber_number = 0
            if(cell == 1||cell == 500||cell == 249501||cell == 250000){
               neighber_number = 3
            } else if((1<cell&&cell<500)||(249500<cell&&cell<250000)||cell%500 == 1||cell%500 == 0){//top
                neighber_number = 5
            }else{//normal
                neighber_number = 8
            }
            val cell_point_count = D_Info(0).toFloat
            val neighber_point_count = D_Info(1).toFloat
            val cell_density = cell_point_count/(neighber_point_count/neighber_number)
            val result = cell.toString + "," + cell_density.toString
		result
            
        }

        val DI = Sum_Cell.map(line=>Cell_density(line))

        DI.persist()

	    //DI.collect().take(50).foreach(println)
        val step2_result = DI.sortBy(line=> line.split(",").apply(1).toFloat)

        val top_50 =  step2_result.collect().take(50)

        var Top_50Map : Map[Int,Float] = Map()

        for (i <- top_50 ){
	var temp = i.split(",")
	val a = temp(0).toInt
	val b = temp(1).toFloat
            Top_50Map += (a->b)
        }


        val GetNeighber = (Cell: String, Top_50Map : Map[Int,Float]) =>{
	    val temp = Cell.split(",")
            val cell = temp(0).toInt
            val cell_density= temp(1).toFloat

            var result = ArrayBuffer[(Int, Float)]()

            if(cell == 1){
                if(Top_50Map.contains(cell+1)){
                    result += (((cell+1),cell_density))
                }else if(Top_50Map.contains(cell+500)){
                    result += (((cell+500),cell_density))
                }else if(Top_50Map.contains(cell+501)){
                    result += (((cell+501),cell_density))
                }
            }else if(cell == 500){
                 if(Top_50Map.contains(cell-1)){
                    result += (((cell-1),cell_density))
                }else if(Top_50Map.contains(cell+499)){
                    result += (((cell+499),cell_density))
                }else if(Top_50Map.contains(cell+500)){
                    result += (((cell+500),cell_density))
                }
                
            }else if(cell == 249501){
                if(Top_50Map.contains(cell+1)){
                    result += (((cell+1),cell_density))
                }else if(Top_50Map.contains(cell-500)){
                    result += (((cell-500),cell_density))
                }else if(Top_50Map.contains(cell-499)){
                    result += (((cell-499),cell_density))
                }
            }else if(cell == 250000){
                if(Top_50Map.contains(cell+1)){
                    result += (((cell+1),cell_density))
                }else if(Top_50Map.contains(cell-500)){
                    result += (((cell-500),cell_density))
                }else if(Top_50Map.contains(cell-501)){
                    result += (((cell-501),cell_density))
                }
               
            }else if(1<cell&&cell<500){//top
                if(Top_50Map.contains(cell-1)){
                    result += (((cell-1),cell_density))
                }else if(Top_50Map.contains(cell + 1)){
                    result += (((cell + 1),cell_density))
                }else if(Top_50Map.contains(cell+500)){
                    result += (((cell+500),cell_density))
                }else if(Top_50Map.contains(cell+499)){
                    result +=(((cell+499),cell_density))
                }else if(Top_50Map.contains(cell+501)){
                    result += (((cell+501),cell_density))
                }
            }else if(249500<cell&&cell<250000){//bottom
                if(Top_50Map.contains(cell-1)){
                    result += (((cell-1),cell_density))
                }else if(Top_50Map.contains(cell + 1)){
                    result += (((cell + 1),cell_density))
                }else if(Top_50Map.contains(cell-500)){
                    result += (((cell-500),cell_density))
                }else if(Top_50Map.contains(cell-499)){
                    result += (((cell-499),cell_density))
                }else if(Top_50Map.contains(cell-501)){
                    result += (((cell-501),cell_density))
                }
                
            }else if(cell%500 == 1){//left
                if(Top_50Map.contains(cell+500)){
                    result +=( (cell+500,cell_density))
                }else if(Top_50Map.contains(cell+1)){
                    result +=( ((cell+1),cell_density))
                }else if(Top_50Map.contains(cell-500)){
                    result += (((cell-500),cell_density))
                }else if(Top_50Map.contains(cell-499)){
                    result +=( ((cell-499),cell_density))
                }else if(Top_50Map.contains(cell+501)){
                    result += (((cell+501),cell_density))
                }
            }else if(cell%500 == 0){//right
                if(Top_50Map.contains(cell+500)){
                    result += (((cell+500),cell_density))
                }else if(Top_50Map.contains(cell - 1)){
                    result += (((cell - 1),cell_density))
                }else if(Top_50Map.contains(cell-500)){
                    result += (((cell-500),cell_density))
                }else if(Top_50Map.contains(cell+499)){
                    result += (((cell+499),cell_density))
                }else if(Top_50Map.contains(cell-501)){
                    result += (((cell-501),cell_density))
                }
            }else{//normal
                if(Top_50Map.contains(cell+1)){
                    result +=(((cell+1),cell_density))
                }else if(Top_50Map.contains(cell - 1)){
                    result += (((cell - 1),cell_density))
                }else if(Top_50Map.contains(cell+500)){
                    result += (((cell+500),cell_density))
                }else if(Top_50Map.contains(cell-500)){
                    result += (((cell-500),cell_density))
                }else if(Top_50Map.contains(cell+501)){
                    result += (((cell+501),cell_density))
                }else if(Top_50Map.contains(cell - 501)){
                    result += (((cell - 501),cell_density))
                }else if(Top_50Map.contains(cell+499)){
                    result += (((cell+499),cell_density))
                }else if(Top_50Map.contains(cell-499)){
                    result += (((cell-499),cell_density))
                }
            }
            result.toArray

        }

        val neighber_density = DI.flatMap {Cell => GetNeighber (Cell , Top_50Map)}
        neighber_density.collect().foreach(println)
         

        }
    
    
    }




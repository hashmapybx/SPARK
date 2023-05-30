package com.zj.sharsbera

import java.util

import org.apache.spark.sql.SparkSession

import scala.io.Source

object ParseVerv {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("bank_data").master("local[4]").getOrCreate()

    //安装行来读取数据
    val shakespeare_path = "D:\\git_respo\\spark_csv\\src\\main\\scala\\sharsbera\\shakespeare.txt"
    val shakespeareRDD = spark.sparkContext.textFile(shakespeare_path)
    val all_verbs_path = "D:\\git_respo\\spark_csv\\src\\main\\scala\\sharsbera\\all_verbs.txt"
    val verb_dict_path = "D:\\git_respo\\spark_csv\\src\\main\\scala\\sharsbera\\verb_dict.txt"
    val all_verbs_RDD = spark.sparkContext.textFile(all_verbs_path).cache()
    val verb_dict_RDD = spark.sparkContext.textFile(verb_dict_path).cache()

    val split_RDD = shakespeareRDD.filter(line=> line.length>0)
//    split_RDD.foreach(println(_))
    val ss = split_RDD.flatMap(line => line.split(" ")).filter(word => word.length>0)
//    split_RDD.foreach(println(_))
    val deal_RDD = ss.map(line => line.trim()).map(line=> line.replaceAll("[\\[.,!:;'?\\]]+","")).map(line => line.toLowerCase)
    //求交集
    val inter_RDD = deal_RDD.intersection(all_verbs_RDD)
    //对交集数据做处理
    val genRDD = inter_RDD.map(word => {
      val lines = parese_data()
      var tmp = ""
      for (word_type <- lines) {
        if(word_type.contains(word)) {
          tmp = word_type.split(",")(0)
        }
      }
      tmp
    })
//    genRDD.foreach(println(_))
    val result = genRDD.filter(word => word.length >0).map((_,1)).reduceByKey(_+_)
    //获取前面10个动词
    result.sortByKey(false).map(x=>(x._2, x._1)).top(10).map(x=>(x._2, x._1)).foreach(println(_))
  }
  
  def parese_data():Array[String] = {
    val fileName= "D:\\git_respo\\spark_csv\\src\\main\\scala\\sharsbera\\verb_dict.txt";  //filepath
    val fileSource = Source.fromFile(fileName)
    val lines = fileSource.getLines().toArray
    fileSource.close()
    lines
  }
}






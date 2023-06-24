package com.zj.ml

import java.util.Properties

import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer

object ChessData {
  /**
   * 关联规则算法FPGRowth 来分析国家象棋开局
   *
   * @param spark
   */
  def deal_data(spark: SparkSession): Unit = {
    val path = "C:\\Users\\Administrator\\Desktop\\games.csv"
    val input_data = spark.read //csv文件加载
      .option("charest", "utf-8")
      .option("header", "true")
      .option("delimiter", ",")
      //访问hdfs上面的文件，在本地做计算
      .csv(path)
    input_data.createOrReplaceTempView("chess_data")
    val sql = "SELECT DISTINCT moves as moves FROM chess_data"
    val url = "jdbc:mysql://localhost:3306/shiyan?characterEncoding=utf8&useSSL=false"
    val properties = new Properties()
    properties.setProperty("user", "root")
    properties.setProperty("password", "1qazZAQ!")
    spark.sql(sql)
      .toDF().write.mode("overwrite").jdbc(url, "chess_data", properties)

  }

  def main(args: Array[String]): Unit = {
    val start_time = System.currentTimeMillis()
    val spark = SparkSession.builder().appName("air")
      .master("local[1]")
//      .master("spark://node01:7077")
      .getOrCreate()
    val path = "C:\\Users\\Administrator\\Desktop\\chess_data.txt"
    val data = spark.sparkContext.textFile(path)
//    val filterData = data.map(line => line.replace("\"", ""))
    val tran: RDD[Array[String]] = data.map(line => {
      val spl_arr = line.split(" ")
      var list = ListBuffer[String]()
      for (w <- spl_arr) {
        list.append(w)
      }
      list.distinct.toIterator.toArray
    })
    //利用spark MLLIb里面的算法 Apriori 实现关联规则分析
    val fpg = new FPGrowth()
      .setMinSupport(0.2) //设置最小执行度
      .setNumPartitions(10) // 分区设置
    val model = fpg.run(tran)

    //设置分区的并行度为1，那么最后输出的文件个数就是一个。
    model.freqItemsets.map(itemset => itemset.items.length).map(w=>(w,1)).reduceByKey((_+_),1)
      .saveAsTextFile("./aaa.txt")

    model.freqItemsets
      .collect().foreach({
      itemset => {
        println(itemset.items.mkString("[", ",", "]" + "," + itemset.freq))
      }
    })
    println("花费时间" + (System.currentTimeMillis() - start_time) + "\n")
  }
}

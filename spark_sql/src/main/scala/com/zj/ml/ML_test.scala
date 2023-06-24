package com.zj.ml

import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object ML_test {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("air").master("local[4]").getOrCreate()
    val data = spark.sparkContext.textFile("D:\\svn\\SPARK\\spark_sql\\src\\main\\resources\\sample_fpgrowth.txt")
    val transactions:RDD[Array[String]] = data.map(s => s.split(" "))


    val fpg = new FPGrowth()
      .setMinSupport(0.2) //设置最小执行度
      .setNumPartitions(10) // 分区设置
    val model = fpg.run(transactions)

    model.freqItemsets.collect().foreach({
      itemset => {
        println(itemset.items.mkString("[",",","]"+","+itemset.freq))
      }
    })

    val minConfidence = 0.8
    model.generateAssociationRules(minConfidence).collect().foreach(rule => {
      println(rule.antecedent.mkString("[",",","]"
      + " => " + rule.consequent.mkString("[",",","]")
        + ","+ rule.confidence
      ))
    })

  }
}

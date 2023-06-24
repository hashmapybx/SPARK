package com.zj.data_analysis

import java.util.Properties

import org.apache.spark.sql.SparkSession

/**
 * 粽子的数据分析
 */

object Zongzi_data {
  def main(args: Array[String]): Unit = {
    val path = "D:\\git_respo\\spark_csv\\src\\main\\resources\\email\\zongzi_data.csv"

    val spark = SparkSession.builder().appName("air").master("local[4]").getOrCreate()
    val input_data = spark.read //csv文件加载
      .option("charest", "utf-8")
      .option("header", "true")
      .option("delimiter", ",")
      //访问hdfs上面的文件，在本地做计算
      .csv(path)
    input_data.show(3)

    input_data.createOrReplaceTempView("zongzi") //将Dataframe转换为表

    //task1 产品价格区间数据统计
    val sql =
      """
        |SELECT price, COUNT(*) AS total_num FROM
        | (
        | SELECT
        |  CASE WHEN CAST(`产品价格` AS DOUBLE) > 0 and  CAST(`产品价格` AS DOUBLE) <= 20 THEN '0-20'
        |   WHEN CAST(`产品价格` AS DOUBLE) > 20 and  CAST(`产品价格` AS DOUBLE) <= 50 THEN '20-50'
        |   WHEN CAST(`产品价格` AS DOUBLE) > 50 and  CAST(`产品价格` AS DOUBLE) <= 100 THEN '50-100'
        |   WHEN CAST(`产品价格` AS DOUBLE) > 100 and  CAST(`产品价格` AS DOUBLE) <= 150 THEN '100-150'
        |   WHEN CAST(`产品价格` AS DOUBLE) > 150 and  CAST(`产品价格` AS DOUBLE) <= 200 THEN '150-200'
        |   WHEN CAST(`产品价格` AS DOUBLE) > 200 and  CAST(`产品价格` AS DOUBLE) <= 500 THEN '200-500'
        |   WHEN CAST(`产品价格` AS DOUBLE) > 500 and  CAST(`产品价格` AS DOUBLE) <= 1000 THEN '500-1000'
        |   WHEN CAST(`产品价格` AS DOUBLE) > 1000 and  CAST(`产品价格` AS DOUBLE) <= 5000 THEN '1000-5000' END price
        | FROM zongzi
        |) a GROUP BY a.price ORDER BY total_num DESC LIMIT 10
        |""".stripMargin

    val sql2 = "SELECT COUNT(*) FROM zongzi WHERE `产品价格` IS NULL"

        spark.sql(sql).toDF().write.format("csv").mode("overwrite")
          .option("sep",",")
          .option("header", "true")
          .save("zongzi_task1.csv")


    
    //todo task2. 品牌人气分布也是搞一个柱形图.
    val sql3 =
      """
        |SELECT a.name as pinpai, sum(CAST(regexp_replace(a.num,'\\+','') AS INT)) as num FROM
        |(
        |SELECT `产品名称` name,substr(`付款人数`,1,instr(`付款人数`, '人')-1) num FROM zongzi where `付款人数` not like '%万%'
        |) a GROUP BY a.name ORDER BY num DESC LIMIT 10
        |""".stripMargin

    val sql4 = "SELECT `产品名称` name,`付款人数`,substr(`付款人数`,1,instr(`付款人数`, '人')-1) num FROM zongzi where `付款人数` not like '%万%'"
    val url = "jdbc:mysql://localhost:3306/shiyan?characterEncoding=utf8&useSSL=false"
    val properties = new Properties()
    properties.setProperty("user", "root")
    properties.setProperty("password", "1qazZAQ!")
    spark.sql(sql3)
      .toDF().write.mode("overwrite").jdbc(url, "zongzi_pinpai", properties)

  }
}

//使用spark框架分析出产品的价格分布柱状图，粽子品牌人气柱状图，还有粽子品牌词云图，
//1.产品的价格分布就是搞一个价格分组的一个柱形图， [0,20],(20,50],(50,100],(100, 150],(150,200](200,,500],(500,1000],(1000,5000],
// (5000, 13168]
//2.品牌人气分布也是搞一个柱形图，
//3.粽子品牌就是弄一个词云图，词云内容就是粽子的品牌名
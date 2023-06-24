package com.zj.jobs

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataTypes, DoubleType, IntegerType, StringType, StructField, StructType}

import scala.util.matching.Regex

/**
 * 这个类是用来做数据处理操作的
 */
object deal_job {


  /**
   * 从工作要求中正则解析出：技能要求
   * @param require
   * @return
   */
  def clean_skill(require:String):String = {
    val pattern = new Regex("[a-zA-Z]+")  // 首字母可以是大写 S 或小写 s
    (pattern findAllIn require).mkString("、")
  }

  /**
   * 公司类型 格式化
   * @param company_type
   * @return
   */
  def clean_company_type(company_type:String):String = {

    if(company_type.length  > 100 || company_type.contains("其他")) {
      "其他"
    } else if (company_type.contains("私营") || company_type.contains("民营")) {
      "民营/私营"
    } else if (company_type.contains("外资") || company_type.contains("外企代表处")) {
      "外资"
    } else if (company_type.contains("合资")) {
      "合资"
    }
    company_type
  }

  /**
   * 行业 格式化。多个行业，取第一个并简单归类
   * @param industry
   * @return
   */
  def clean_industry(industry:String):String = {
    if (industry.length >100 || industry.contains("其他")) {
      "其他"
    }
    val industry_map = Map[String,String]("IT互联网"->"互联网|计算机|网络游戏", "房地产"-> "房地产", "电子技术"-> "电子技术", "建筑"-> "建筑|装潢",
      "教育培训"-> "教育|培训", "批发零售"-> "批发|零售", "金融"-> "金融|银行|保险", "住宿餐饮"-> "餐饮|酒店|食品",
      "农林牧渔"-> "农|林|牧|渔", "影视文娱"-> "影视|媒体|艺术|广告|公关|办公|娱乐", "医疗保健"-> "医疗|美容|制药",
      "物流运输"->"物流|运输", "电信通信"-> "电信|通信", "生活服务"-> "人力|中介")
    for (elem <- industry_map) {
      if (industry.contains(elem._2)) {
        elem._1
      }
    }
    industry.split('、')(0).toString.replace("/","")
  }


  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("air").master("local[1]").getOrCreate()
    val schema = new StructType(Array(
      StructField("position",DataTypes.StringType),
      StructField("num",DataTypes.StringType),
      StructField("company",DataTypes.StringType),
      StructField("job_type",DataTypes.StringType),
      StructField("jobage",DataTypes.StringType),
      StructField("lang",DataTypes.StringType),
      StructField("age",DataTypes.StringType),
      StructField("sex",DataTypes.StringType),
      StructField("education",DataTypes.StringType),
      StructField("workplace",DataTypes.StringType),
      StructField("worktime",DataTypes.StringType),
      StructField("salary",DataTypes.StringType),
      StructField("welfare",DataTypes.StringType),
      StructField("hr",DataTypes.StringType),
      StructField("phone",DataTypes.StringType),
      StructField("address",DataTypes.StringType),
      StructField("company_type",DataTypes.StringType),
      StructField("industry",DataTypes.StringType),
      StructField("require",DataTypes.StringType),
      StructField("worktime_day",DataTypes.StringType),
      StructField("worktime_week",DataTypes.StringType),
      StructField("skill",DataTypes.StringType)
    ))
    val input_data = spark.read.schema(schema)
      .option("charest", "utf-8")
      .option("header", "false")
      .option("delimiter", ",")
      //访问hdfs上面的文件，在本地做计算
      .csv("D:\\svn\\SPARK\\spark_sql\\src\\main\\scala\\com\\zj\\jobs\\job.csv")

//    //删除重复数据
//    val new_data = input_data.dropDuplicates()
//    new_data.show(2)

    input_data.createOrReplaceTempView("job_info")

    spark.udf.register("clean_industry",(ind:String)=>clean_industry(ind))
    spark.udf.register("clean_skill",(ind:String)=>clean_skill(ind))
    spark.udf.register("clean_company_type",(ind:String)=>clean_company_type(ind))
    //todo 数据处理要求
    //招聘人数处理 null 为1. 一般是一人; 若干人当成 3人
    //年龄要求：缺失值填 无限；格式化
    // 语言要求: 忽视精通程度，格式化
    // 月薪: 格式化。根据一般经验取低值，比如 5000-6000, 取 5000
    val sql1 =
      """
        |SELECT `position`,
        | CASE WHEN `num` IS NULL THEN '1'
        |  WHEN  `num` LIKE '%若干%' THEN 3 ELSE `num` end AS num,
        | `company`,
        |  regexp_replace(`job_type`,'毕业生见习','实习') as job_type ,
        |  CASE WHEN `jobage` IN ('应届生', '不限') THEN jobage
        |  ELSE jobage end as jobage,
        |  CASE WHEN `lang` IS NULL THEN '不限'
        |  WHEN `lang` LIKE '%水平%' THEN SPLIT(`lang`, '水平')[0]
        |  WHEN `lang` LIKE '%其他%' THEN '不限' ELSE `lang` end AS lang,
        |   CASE WHEN `age` IS NULL THEN '不限'
        |   WHEN `age` LIKE '%岁至%' THEN regexp_replace(age,'岁至','-')
        |   WHEN `age` LIKE '%岁%' THEN regexp_replace(age,'岁','') ELSE `age` end AS age,
        | regexp_replace(`sex`,'无','不限') as sex,
        | `education`,
        | `workplace`,
        | `worktime`,
        | CASE WHEN `salary` LIKE '%参考月薪： %' THEN regexp_replace(salary,'参考月薪： ','')
        | WHEN `salary` LIKE '%-%' THEN SPLIT(`salary`,'-')[0]
        | WHEN `salary` IS NULL THEN '0' ELSE `salary` end as salary,
        | `welfare`,
        | `hr`,
        | `phone`,
        | `address`,
        |  clean_company_type(`company_type`) AS company_type,
        | clean_industry(`industry`) as industry,
        |  `require`,
        | CASE WHEN worktime LIKE '%小时%' THEN SPLIT(worktime,'小时')[0] ELSE 'O' END AS worktime_day,
        | CASE WHEN worktime LIKE '%周%' THEN regexp_extract(worktime,'\S*周',0) ELSE 'O' END AS worktime_week,
        | clean_skill(require) as skill
        |   FROM job_info
        |""".stripMargin
    val sql2 =
      """
        |SELECT * FROM job_info where num is null
        |""".stripMargin
    spark.sql(sql1).show(3)
//      .toDF().write.format("csv").mode("overwrite")
//      .option("sep",",")
//      .option("header", "true")
//      .option("nullValue","nn")
//      .save("clean_job.cav")

  }
}

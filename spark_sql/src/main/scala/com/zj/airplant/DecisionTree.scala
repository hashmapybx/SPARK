package com.zj.airplant

import java.util.Properties

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

//决策树算法来分析民航订单数据
object DecisionTree {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("air").master("local[4]").getOrCreate()

    val path = "D:\\git_respo\\spark_csv\\src\\main\\resources\\民航数据.csv"
    //    val dataRDD = spark.sparkContext.textFile(path)
    //      .map(line => line.split(",")).map(x => Row(x(0), x(1), x(2), x(3), x(4)))
    //
    //    val schema = StructType(
    //      Seq(
    //        StructField("dayy", StringType, false),
    //        StructField("saler", StringType, false),
    //        StructField("buyer", StringType, false),
    //        StructField("num", StringType, false),
    //        StructField("money", StringType, false)
    //      )
    //    )
    //    //转化成Dataframe
    //    val df = spark.createDataFrame(dataRDD, schema)
    //    print(df.schema)
    //    df.createOrReplaceTempView("air_info")
    //    df.createOrReplaceTempView("air_info")
    val df = spark.read.csv(path)
    .withColumnRenamed("_c0", "dayy")
    .withColumnRenamed("_c1", "saler")
    .withColumnRenamed("_c2", "buyer")
    .withColumnRenamed("_c3", "num")
    .withColumnRenamed("_c4", "money")
    print(df.printSchema())
    val new_df = df.select(df.col("dayy").cast(IntegerType).as("dayy"),df.col("num").cast(IntegerType).as("num"),
      df.col("money").cast(IntegerType).as("money"))
//    print(new_df.head(3))
    new_df.show(3)


    val splitRDD = new_df.randomSplit(Array(0.8, 0.2))
    val (train, test) = (splitRDD(0), splitRDD(1))
    //    day,saler,buyer,num,money
    val traindf = train.withColumnRenamed("dayy", "label")
//    val saler = new StringIndexer().setInputCol("saler").setOutputCol("saler_")
//    val buyer = new StringIndexer().setInputCol("buyer").setOutputCol("buyer_")
    //将多个列特征合并在一起
    val assembler = new VectorAssembler().setInputCols(Array("num", "money")).setOutputCol("features")

    import org.apache.spark.ml.Pipeline

    val rf = new RandomForestRegressor().setLabelCol("label").setFeaturesCol("features")

    val pipeline = new Pipeline().setStages(Array(assembler, rf))
    val model = pipeline.fit(traindf)

    val testDf = test.withColumnRenamed("dayy", "label")
    val labelsAndPredictions = model.transform(testDf)
    val prop = new Properties()
    prop.setProperty("user", "root")
    prop.setProperty("password", "1qazZAQ!")
    //将预测的结果数据写到mysql里面。
    labelsAndPredictions.select(labelsAndPredictions.col(
      "prediction").cast(StringType),
      labelsAndPredictions.col("label").cast(StringType),
      labelsAndPredictions.col("features").cast(StringType))
      .write.mode(SaveMode.Overwrite).jdbc("jdbc:mysql://localhost:3306/bike", "air_predict", prop)
//      .distinct().show(100,false)
    val eva = new RegressionEvaluator().setLabelCol("label").setPredictionCol("prediction")
    val accuracy = eva.evaluate(labelsAndPredictions)
    println(accuracy)
  }
}

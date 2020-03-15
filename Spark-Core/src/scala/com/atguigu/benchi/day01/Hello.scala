package com.atguigu.benchi.day01
import org.apache.spark.{SparkConf,SparkContext}
/**
  *第一个spark测试项目
  */
object Hello {
    def main(args: Array[String]): Unit = {
        //1.创建SparkContext对象,用于连接spark,打包时,把master的设置去掉,在提交的时候,使用-- master来设置master
        val conf = new SparkConf().setMaster("local[2]").setAppName("Hello")


        val sc = new SparkContext(conf)
        //2.从数据源得到RDD
        val lineRDD = sc.textFile(args(0))
        //3.对RDD做各种转换
        val resultRDD = lineRDD.flatMap(_.split("\\W")).map((_,1)).reduceByKey(_ + _)
        //4.执行一个行动算子,(collect:将各个节点计算后的数据,拉取到驱动端)
        val wordCount = resultRDD.collect()
        wordCount.foreach(println)
        //5.关闭SparkContext
        sc.stop()

    }
}

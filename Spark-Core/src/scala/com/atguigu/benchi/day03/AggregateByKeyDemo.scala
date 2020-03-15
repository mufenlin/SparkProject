package com.atguigu.benchi.day03

import org.apache.spark.{SparkConf, SparkContext}

/**
  *AggregateByKey算子:分区内聚合算子和分区间聚合算子不一样
  *
  */
object AggregateByKeyDemo {
    def main(args: Array[String]): Unit = {
        val  conf = new SparkConf().setMaster("local[2]").setAppName("PartitionBy")
        val sc: SparkContext = new SparkContext(conf)
        val list1 = List(("a",3),("a",2),("c",4),("b",3),("c",6),("c",8))
        val rdd= sc.parallelize(list1,2)

        //1)分区内求最大值,2)分区间求和
        rdd.aggregateByKey(Int.MinValue)((u,v) => u.max(v),(u1,u2) => u1+ u2)

    }
}

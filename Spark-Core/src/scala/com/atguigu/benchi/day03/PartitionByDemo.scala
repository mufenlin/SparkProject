package com.atguigu.benchi.day03

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
  *PartitionBy算子:对RDD进行分区操作,原有分区器与传入的分区器相同,则返回原有分区器,否则会生成ShuffleRDD
  */
object PartitionByDemo {
    def main(args: Array[String]): Unit = {
        val  conf = new SparkConf().setMaster("local[2]").setAppName("PartitionBy")
        val sc: SparkContext = new SparkContext(conf)
        val list1 = List(30, 50, 70, 60, 10, 20)
        val rdd1 = sc.parallelize(list1, 2)
        val rdd2 = rdd1.map((_,1))
        println(rdd2.partitioner)
/*        val rdd3 = rdd2.partitionBy(new HashPartitioner(3))
        println(rdd3.partitioner)*/

        //如果按照value分区
        //调换KV
        val rdd3 = rdd2.map{
            case (k,v) => (v,k)
        }.partitionBy(new HashPartitioner(5)).map{
            case(k,v) => (k,v)
        }

        rdd3.glom().collect.map(_.toList).foreach(println)
        sc.stop()



    }
}

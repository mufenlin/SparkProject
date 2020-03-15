package com.atguigu.benchi.day02.doublevalue

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Zip拉链算子
  * rdd.zip()  前提:
  *    ①分区数一样;
  *    ②分区中的元素个数一样
  *  rdd.zipPartition()(()=>{})  前提
  *    ①分区数一样;
  */
object ZipDemo {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[2]").setAppName("Zip")
        val sc =new SparkContext(conf)
        val list1 = List(30, 50, 73, 66, 19,80,90)
        val list2 = List(100, 50, 60, 58, 20,80)
        val rdd1 = sc.parallelize(list1,2)
        val rdd2 = sc.parallelize(list2,2)
        //一般拉链,
//        val rddRes = rdd1.zip(rdd2)


        val rddRes = rdd1.zipPartitions(rdd2)((it1,it2) => {
            it1.zip(it2)
        })


        rddRes.collect.foreach(println)

        sc.stop()

    }
}

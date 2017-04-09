package com.smule.ML

import java.util.Vector.VectorSpliterator

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.{Vector, Vectors}

/**
  * Created by vincenty on 4/4/17.
  */
object simularity extends App {
  val clientArgs = new ClientArgs(args)
  val conf = new SparkConf()
  if (!clientArgs.master.isEmpty) conf.setMaster(clientArgs.master)
  if (!clientArgs.appName.isEmpty) conf.setAppName(clientArgs.appName)
  val sc = SparkContext.getOrCreate(conf)
  val userItem = sc.textFile(clientArgs.inputFile).map(_.split("\t")).map { case (Array(user, item, xs@_*)) => (item, user) }
  val itemDic = userItem.map(_._1).distinct().zipWithIndex().mapValues(_.toInt)
  val itemIdDic = itemDic.map(item => (item._2, item._1));
  val maxId = itemDic.count().toInt;
  val userItemId = userItem.join(itemDic).map { case (item, (user, itemId)) => (user, itemId) }

  val matrix = new RowMatrix(userItemId.groupByKey()
    .mapValues(items => {
      val itemRates = items.groupBy(v => v).mapValues(_.size.toDouble)
      //normalize rates
      val mean = itemRates.values.sum/itemRates.count(v => true)
      itemRates.mapValues(rate => rate - mean)
      Vectors.sparse(maxId, itemRates.toSeq)
    })
    .values)

  val itemsSim = matrix.columnSimilarities();

  itemsSim.entries.map(entry => (entry.i.toInt, (entry.j.toInt, entry.value))).join(itemIdDic)
    .map { case (i, ((j, value), itemi)) => (j, (itemi, value)) }
    .join(itemIdDic).map { case (j, ((itemi, value), itemj)) => (itemi, itemj, value) }
    .saveAsTextFile(clientArgs.outputFile);


  class ClientArgs(args: Array[String]) {
    var master: String = ""
    var appName: String = ""
    var inputFile: String = _
    var outputFile: String = _

    parse(args.toList)

    def parse(args: List[String]): Unit = args match {
      case ("--master") :: value :: tail =>
        master = value
        parse(tail)
      case ("--appName") :: value :: tail =>
        appName = value
        parse((tail))
      case inputFile :: outputFile :: nil =>
        this.inputFile = inputFile
        this.outputFile = outputFile
      case _ =>
        println("usage: --master local -- appName [application name] inputFile outputFile")
        System.exit(-1)
    }
  }

}

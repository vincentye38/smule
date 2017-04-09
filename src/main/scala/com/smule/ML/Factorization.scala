package com.smule.ML

import com.smule.ML.simularity.args
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by vincenty on 4/6/17.
  */
object Factorization extends App{
  val clientArgs = new ClientArgs(args)
  val conf = new SparkConf()
  if (!clientArgs.master.isEmpty) conf.setMaster(clientArgs.master)
  if (!clientArgs.appName.isEmpty) conf.setAppName(clientArgs.appName)
  val sc = SparkContext.getOrCreate(conf)
  val userItem = sc.textFile(clientArgs.inputFile).map(_.split("\t")).map { case (Array(user, item, xs@_*)) => (item, user) }
  val itemDic = userItem.map(_._1).distinct().zipWithIndex().mapValues(_.toInt)
  itemDic.saveAsTextFile(clientArgs.dicFile)

  val ratings = userItem.join(itemDic)
    .map { case (item, (user, itemId)) => ((user.toInt, itemId),1) }
    .groupByKey().mapValues(_.reduce(_+_).toDouble)
    .map{case ((userId, itemId), rate) => (userId, (itemId, rate))}
//    .groupByKey().mapValues(v => {val rates = v.map(_._2); val mean = rates.sum/ rates.count(v => true); v.map{case (itemId, rate) => (itemId, rate - mean)}})
//      .flatMapValues(_.toList)
      .map{case (userId, (itemId, rate)) => Rating(userId, itemId, rate)}

  val model = ALS.train(ratings, clientArgs.rank, clientArgs.numIterations)

  model.save(sc, clientArgs.outputFile)

  class ClientArgs(args: Array[String]) {
    var master: String = ""
    var appName: String = ""
    var inputFile: String = _
    var outputFile: String = _
    var dicFile: String = _
    var rank = 10
    var numIterations = 10

    parse(args.toList)

    def parse(args: List[String]): Unit = args match {
      case ("--master") :: value :: tail =>
        master = value
        parse(tail)
      case ("--appName") :: value :: tail =>
        appName = value
        parse((tail))
      case ("--rank") :: value :: tail =>
        rank = value.toInt
        parse(tail)
      case ("--numIterations") :: value :: tail =>
        numIterations = value.toInt
        parse(tail)
      case inputFile :: outputFile :: dicFile:: nil =>
        this.inputFile = inputFile
        this.outputFile = outputFile
        this.dicFile = dicFile
      case _ =>
        println("usage: --master local -- appName [application name] --rank [int] --numIterations [int] inputFile outputFile")
        System.exit(-1)
    }
  }
}

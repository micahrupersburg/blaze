package gdg.blaze.ext.io

import java.util.concurrent.ExecutorService

import gdg.blaze.{Message, BlazeContext, PluginConfig}
import gdg.blaze.codec.PlainCodec
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

import scala.collection.mutable

class SourceInput(pc: PluginConfig, bc: BlazeContext, queue: mutable.Queue[RDD[String]] = mutable.Queue(), pool:ExecutorService = java.util.concurrent.Executors.newFixedThreadPool(1) ) extends ((scala.io.Source) => DStream[String]) {
  override def apply(source:scala.io.Source): DStream[String] = {
    val out = bc.sc.queueStream(queue = queue, oneAtATime = false)
    val codec = pc.getCodec("codec")(bc).getOrElse(PlainCodec.single)
    pool.execute(new Runnable {
      override def run() = {
        val stream = scala.io.Source.stdin
        stream.getLines().foreach{ x =>
          val rdd = bc.sc.sparkContext.makeRDD(Seq(x))
          queue.enqueue(rdd)
        }
      }
    })
    out
  }

}

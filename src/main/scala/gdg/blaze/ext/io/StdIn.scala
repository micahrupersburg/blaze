package gdg.blaze.ext.io

import java.net.ServerSocket
import java.util.concurrent.ExecutorService

import gdg.blaze.codec.{PlainCodec, JSONCodec}
import gdg.blaze._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

import scala.collection.mutable

class StdIn (pc: PluginConfig, bc: BlazeContext, source:SourceInput) extends Input {
  override def apply(): DStream[Message] = {
    val codec = pc.getCodec("codec")(bc).getOrElse(PlainCodec.single)
    val port = pc.getInt("port").getOrElse(4000)
    source(scala.io.Source.stdin).flatMap(codec.decode)
  }
}

object StdIn extends PluginFactory[StdIn] {
  override def apply(pc: PluginConfig, bc: BlazeContext): StdIn = new StdIn(pc, bc, new SourceInput(pc, bc))
}

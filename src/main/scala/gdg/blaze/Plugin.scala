package gdg.blaze


import java.util.concurrent.ExecutorService

import gdg.blaze.Message
import gdg.blaze.codec.{PlainCodec, JSONCodec}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

import scala.collection.mutable

trait Plugin extends Serializable

trait Filter extends Plugin with ((DStream[Message]) => DStream[Message])

trait Input extends Plugin with (() => DStream[Message])

trait Codec extends Plugin {
  def encode(message: Message) : String
  def decode(str: String) : Traversable[Message]
}

trait Output extends Plugin with (DStream[Message] => Unit)

trait PluginFactory[T <: Plugin] extends ((PluginConfig, BlazeContext) => T)

abstract class BasicInput(config: PluginConfig, bc:BlazeContext) extends Input {
  override def apply(): DStream[Message] = {
    val codec = config.getCodec("codec")(bc).getOrElse(PlainCodec.single)
    input().flatMap(codec.decode)
  }
  def input() : DStream[String]
}

abstract class BasicFilter(config: PluginConfig, bc:BlazeContext) extends Filter {
  override def apply(dStream: DStream[Message]): DStream[Message] = {
    dStream.flatMap { msg =>
      if(bc.filter.apply(msg)) {
        transform(msg)
      } else {
        Some(msg)
      }
    }
  }

  def transform(msg: Message): Traversable[Message]
}

class MessageFilter(filters: Seq[(Message) => Boolean] = Seq()) extends ((Message) => Boolean) {
  def apply(msg: Message): Boolean = {
    for(f <- filters) {
      if(!f.apply(msg)) {
        return false
      }
    }
    true
  }
}


package gdg.blaze


import gdg.blaze.codec.PlainCodec
import org.apache.spark.streaming.dstream.DStream


trait Plugin extends Serializable

trait Filter extends Plugin with ((DStream[Message]) => DStream[Message])

trait Input extends Plugin with (() => DStream[Message])
trait CodecFactory[T <:Codec] extends ((PluginConfig) => T)
trait Codec extends Plugin {
  def encode(message: Message) : String
  def decode(str: String) : Traversable[Message]
}

trait Output extends Plugin with (DStream[Message] => Unit)

trait PluginFactory[T <: Plugin] extends ((PluginConfig, BlazeContext) => T)

case class BasicConfig(var codec: Codec = PlainCodec.single)
abstract class BasicInput(config: BasicConfig) extends Input {
  override def apply(): DStream[Message] = {
    input().flatMap(config.codec.decode)
  }
  def input() : DStream[String]
}

abstract class BasicFilter extends Filter {
  val filter:MessageFilter
  override def apply(dStream: DStream[Message]): DStream[Message] = {
    dStream.flatMap { msg =>
      if(filter(msg)) {
        transform(msg)
      } else {
        Some(msg)
      }
    }
  }

  def transform(msg: Message): Traversable[Message]
}

trait FilterFunction extends ((Message) => Boolean) with Serializable
class MessageFilter(filters: Seq[FilterFunction] = Seq()) extends ((Message) => Boolean) with Serializable {
  def apply(msg: Message): Boolean = {
    for(f <- filters) {
      if(!f.apply(msg)) {
        return false
      }
    }
    true
  }
}


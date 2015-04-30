package gdg.blaze


import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

trait Plugin extends Serializable

trait Filter extends Plugin with ((DStream[Message]) => DStream[Message])

trait Input extends Plugin with (() => DStream[Message])

trait Codec extends Plugin with ((Message) => Traversable[Message])

trait Output extends Plugin with (DStream[Message] => Unit)

trait PluginFactory[T <: Plugin] extends ((PluginConfig, BlazeContext) => T)

abstract class BasicFilter(config: PluginConfig, bc:BlazeContext) extends Filter {
  override def apply(dStream: DStream[Message]): DStream[Message] = {
    dStream.flatMap { msg =>
      if(bc.filter(msg)) {
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


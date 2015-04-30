package gdg.blaze.ext

import gdg.blaze._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream


class GrokFilter(config: PluginConfig, bc:BlazeContext) extends BasicFilter(config, bc) {
  override def transform(msg: Message): Traversable[Message] = {
    None
  }
}

object GrokFilter extends PluginFactory[GrokFilter] {
  override def apply(config: PluginConfig, bc:BlazeContext) = new GrokFilter(config, bc)
}

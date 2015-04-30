package gdg.blaze.ext.kafka

import gdg.blaze._
import org.apache.spark.streaming.dstream.DStream

class KafkaInput(pc: PluginConfig, bc: BlazeContext) extends Input {
  override def apply(): DStream[Message] = ???
}

object KafkaInput extends PluginFactory[KafkaInput] {
  override def apply(pc: PluginConfig, bc: BlazeContext): KafkaInput = new KafkaInput(pc, bc)
}

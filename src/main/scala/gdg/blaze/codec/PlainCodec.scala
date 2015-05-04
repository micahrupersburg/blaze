package gdg.blaze.codec

import gdg.blaze._

class PlainCodec extends Codec {
  override def decode(str: String): Traversable[Message] =  Some(new Message(message = str))
  override def encode(message: Message): String = message.toString
}

object PlainCodec extends CodecFactory[PlainCodec] {
  val single = new PlainCodec()
  override def apply(config: PluginConfig): PlainCodec = single
}
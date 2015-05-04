package gdg.blaze.codec

import java.util

import com.fasterxml.jackson.databind.ObjectMapper
import gdg.blaze._

import collection.JavaConverters._

class JSONCodec(config: PluginConfig = PluginConfig.empty, mapper: ObjectMapper = new ObjectMapper()) extends Codec {

  override def decode(str: String): Traversable[Message] = message(str)

  override def encode(message: Message): String = mapper.writeValueAsString(message)

  def message(json: String): Traversable[Message] = {
    if(json.startsWith("[")) {
      mapper.readValue(json, classOf[util.List[util.Map[String, _]]]).asScala.map { v =>
        new Message(v.asScala.toMap)
      }
    } else {
      val x: util.Map[String, _] = mapper.readValue(json, classOf[util.Map[String, _]])
      Some(new Message(x.asScala.toMap))
    }
  }

}

object JSONCodec extends CodecFactory[JSONCodec] {

  override def apply(config: PluginConfig): JSONCodec = new JSONCodec(config)
}


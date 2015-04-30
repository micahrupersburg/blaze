package gdg.blaze.codec

import java.util

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.ObjectMapper
import gdg.blaze._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

import collection.JavaConverters._

class JSONCodec(config: PluginConfig =PluginConfig.empty, source: String = "message", mapper: ObjectMapper = new ObjectMapper()) extends Codec {

  override def apply(m: Message): Traversable[Message] = {
    m.getString(source) match {
      case x if x.isDefined => message(x.get)
      case _ => None
    }
  }

  def message(json: String): Traversable[Message] = {
    if(json.startsWith("[")) {
      mapper.readValue(json, classOf[util.List[util.Map[String, _]]]).asScala.map { v =>
        new Message(data = v.asScala.toMap)
      }
    } else {
      val x: util.Map[String, _] = mapper.readValue(json, classOf[util.Map[String, _]])
      Some(new Message(data = x.asScala.toMap))
    }
  }

}

object JSONCodec extends PluginFactory[JSONCodec] {
  override def apply(config: PluginConfig, sc: BlazeContext): JSONCodec = new JSONCodec(config)
}


package gdg.blaze.codec

import java.util

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.ObjectMapper
import gdg.blaze.{Message, StringCodec}

import collection.JavaConverters._

class JSONCodec(val mapper: ObjectMapper = new ObjectMapper()) extends StringCodec {
  override def transform(json: String): Traversable[Message] = {
    if(json.startsWith("[")) {
      mapper.readValue(json, classOf[util.List[util.Map[String, _]]]).asScala.map{ v=>
        new Message(data=v.asScala.toMap)
      }
    } else {
      val x: util.Map[String, _] = mapper.readValue(json, classOf[util.Map[String, _]])
      Some(new Message(data=x.asScala.toMap))
    }
  }
}


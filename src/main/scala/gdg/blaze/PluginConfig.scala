package gdg.blaze


import java.util

import com.fasterxml.jackson.core.{JsonParser, JsonToken, Version}
import com.fasterxml.jackson.databind.Module.SetupContext
import com.fasterxml.jackson.databind._
import com.fasterxml.jackson.databind.`type`.MapLikeType
import com.fasterxml.jackson.databind.deser.BeanDeserializerModifier
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.databind.node._
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import collection.JavaConverters._

object PluginConfig extends (Traversable[Member] => PluginConfig) {
  def empty = new PluginConfig()

  override def apply(value: Traversable[Member]): PluginConfig = new PluginConfig(Converter(value))
}

class InterString(value: String) extends (Message => String) {
  override def apply(v1: Message): String = ???
}

object Converter extends (Traversable[Member] => Map[String, Any]) {
  def apply(members: Traversable[Member]): Map[String, Any] = {
    members.groupBy(_.key).mapValues { vs =>
      val t = vs.map(_.value) match {
        case x if x.size == 1 => x.head
        case x => new ArrayValue(x.toSeq)
      }
      convert(t)
    }
  }

  def convert: Value => Any = {
    case v: SingleBool => v.value
    case v: SingleFloat => v.value
    case v: SingleString => v.value
    case v: ArrayValue => v.value.map(convert)
    case v: ObjectValue => apply(v.value)
    case v: NamedObjectValue =>
      val fmap: Map[String, Any] = apply(v.value)
      Map("@type" -> v.name, "@fields" -> fmap)
  }

}

object Mapper {
  val mapper: ObjectMapper = new ObjectMapper()
  @transient lazy val codecDes = new StdDeserializer[Codec](classOf[Codec]) {
    override def deserialize(p: JsonParser, ctxt: DeserializationContext): Codec = {
      val out = p.getCurrentToken match {
        case JsonToken.VALUE_STRING => Registry.codec(p.getValueAsString, new PluginConfig())
        case JsonToken.START_OBJECT =>
          val map = p.readValueAs(classOf[Map[String, Any]])
          val fields: Map[String, Any] = map.get("@fields") match {
            case m: Map[String, Any] => m
          }
          val name = map.get("@type").toString
          Registry.codec(name, new PluginConfig(fields))
        case _ => throw new IllegalStateException("Unknown JSON TOKEN FOR CODEC ")
      }
      out
    }
  }
  @transient lazy val mapAnyVal = new StdDeserializer[Map[String, _]](classOf[Map[String, _]]) {
    def conv(node: JsonNode): Any = {
      node match {
        case x: ArrayNode =>
          x.elements().asScala.map(conv).toList
        case x: ObjectNode => x.fields().asScala.map { en => (en.getKey, conv(en.getValue)) }.toMap
        case x: ValueNode => x.asText()
      }
    }

    override def deserialize(p: JsonParser, ctxt: DeserializationContext): Map[String, _] = {
      val tree: JsonNode = p.readValueAsTree()
      tree match {
        case x:ArrayNode =>
          val l: List[Any] = conv(x).asInstanceOf
          if((!l.isEmpty) && l.head.isInstanceOf[List[_]]) {
            l.asInstanceOf[List[List[_]]].map { t => (t(0), t(1)) }.groupBy(_._1).mapValues(_.map(_._2)).asInstanceOf
          } else {
            l.sliding(2, 2).map { t => (t(0), t(1)) }.toMap.mapValues(List(_)).asInstanceOf
          }
        case x:ObjectNode => conv(x).asInstanceOf
        case _ => throw new IllegalStateException(s"can't deserialize : $tree")
      }
    }
  }

  val blazeModule = new SimpleModule("MyModule", new Version(1, 0, 0, null, "gdg", "blaze")) {
    override def setupModule(context: SetupContext): Unit = {
      super.setupModule(context)
      context.addBeanDeserializerModifier(new BeanDeserializerModifier {
        override def modifyMapLikeDeserializer(config: DeserializationConfig, `type`: MapLikeType, beanDesc: BeanDescription, deserializer: JsonDeserializer[_]): JsonDeserializer[_] = {
          super.modifyMapLikeDeserializer(config, `type`, beanDesc, deserializer)
        }
      })
    }
  }


    .addDeserializer(classOf[Codec], codecDes)
//    .addDeserializer(classOf[List[_]], listAnyValDes)
//    .addDeserializer(classOf[Map[String, _]], mapAnyVal)

  mapper.registerModule(DefaultScalaModule)
  mapper.registerModule(blazeModule)
  mapper.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true)
  mapper.configure(DeserializationFeature.UNWRAP_SINGLE_VALUE_ARRAYS, true)
  def get(): ObjectMapper = {
    mapper
  }
}

class PluginConfig(config: Map[String, Any] = Map()) extends Serializable {
  @transient lazy val mapper: ObjectMapper = Mapper.get()

  def convert[V](clazz: Class[V]): V = {
    mapper.convertValue(config, clazz)
  }

  def getInt(name: String): Option[Int] = {
    getNum(name).map(_.toInt)
  }

  def get(name: String): Option[Any] = config.get(name)

  def one(name: String): Option[Any] = config.get(name)

  def getNum(name: String): Option[Double] = {
    one(name).map {
      case x: Number => x.doubleValue()
      case _ => throw new IllegalStateException()
    }
  }

  def getString(name: String): Option[String] = {
    one(name).map {
      case x: String => x
      case _ => throw new IllegalStateException()
    }
  }

  def getInterString(name: String): Message => Option[String] = {
    val v = one(name)
    if(v.isDefined) {
      v.get match {
        case x: String => _: Message => Some(x)
        case _ => throw new IllegalStateException()
      }
    } else {
      Message => None
    }
  }

  def getBool(name: String): Option[Boolean] = {
    one(name).map {
      case x: Boolean => x
      case x: String => x.toString.toBoolean
      case _ => throw new IllegalStateException()
    }
  }
}
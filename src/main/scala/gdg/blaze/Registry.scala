package gdg.blaze

import gdg.blaze.codec.{PlainCodec, JSONCodec}
import gdg.blaze.ext.es.ElasticSearchOutput
import gdg.blaze.ext.hadoop.HdfsInput
import gdg.blaze.ext.io.{StdIn, TCPInput, StdOut}
import gdg.blaze.ext.GrokFilter
import gdg.blaze.ext.kafka.KafkaInput
import gdg.blaze.godaddy.FeedInput

object Registry {

  private val inputs: Map[String, PluginFactory[_ <: Input]] = Map(
    "hdfs" -> HdfsInput,
    "tcp" -> TCPInput,
    "kafka" -> KafkaInput,
    "stdin" -> StdIn,
    "feed" -> FeedInput
  )
  private val outputs: Map[String, PluginFactory[_ <: Output]] = Map(
    "elasticsearch" -> ElasticSearchOutput,
    "stdout" -> StdOut
  )
  private val filters: Map[String, PluginFactory[_ <: Filter]] = Map(
    "grok" -> GrokFilter
  )
  private val codecs: Map[String, PluginFactory[_ <: Codec]] = Map(
    "json" -> JSONCodec,
    "plain" -> PlainCodec
  )

  def input(value: NamedObjectValue, bc: BlazeContext) = {
    inputs.get(value.name).map(_(PluginConfig(value.value), bc)).getOrElse(throw new IllegalStateException(s"Missing Value ${value.name}"))
  }

  def output(value: NamedObjectValue, bc: BlazeContext) = {
    outputs.get(value.name).map(_(PluginConfig(value.value), bc)).getOrElse(throw new IllegalStateException(s"Missing Value ${value.name}"))
  }

  def filter(value: NamedObjectValue, bc: BlazeContext): Filter = {
    filters.get(value.name).map(_(PluginConfig(value.value), bc)).getOrElse(throw new IllegalStateException(s"Missing Value ${value.name}"))
  }

  def codec(value: NamedObjectValue, bc: BlazeContext) = {
    codecs.get(value.name).map(_(PluginConfig(value.value), bc)).getOrElse(throw new IllegalStateException(s"Missing Value ${value.name}"))
  }

}

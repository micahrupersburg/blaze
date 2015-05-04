package gdg.blaze.ext.io

import gdg.blaze._
import org.apache.spark.streaming.dstream.DStream

class StdIn (pc: StdInConfig, source:SourceInput) extends Input {
  override def apply(): DStream[Message] = {
    source(scala.io.Source.stdin).flatMap(pc.codec.decode)
  }
}
case class StdInConfig(codec:Codec)
object StdIn extends PluginFactory[StdIn] {
  override def apply(pc: PluginConfig, bc: BlazeContext): StdIn = new StdIn(pc.convert(classOf[StdInConfig]), new SourceInput(bc))
}

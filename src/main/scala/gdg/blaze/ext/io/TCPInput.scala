package gdg.blaze.ext.io

import java.net.ServerSocket

import gdg.blaze._
import org.apache.spark.streaming.dstream.DStream

class TCPInput(pc: TCPInputConfig, source: SourceInput) extends Input {

  override def apply(): DStream[Message] = {
    pc.mode match {
      case "server" => server()
      case "client" => throw new IllegalStateException("Client mode not yet supported")
    }
  }

  def server(): DStream[Message] = {
    val ss = new ServerSocket(pc.port)
    val socket = ss.accept()
    val stream = scala.io.Source.fromInputStream(socket.getInputStream)
    source(stream).flatMap(pc.codec.decode)
  }
}

case class TCPInputConfig(
                         codec: Codec,
                         port:Int,
                         mode:String
                           )
object TCPInput extends PluginFactory[TCPInput] {
  override def apply(pc: PluginConfig, bc: BlazeContext): TCPInput = new TCPInput(pc.convert(classOf[TCPInputConfig]), new SourceInput(bc))
}

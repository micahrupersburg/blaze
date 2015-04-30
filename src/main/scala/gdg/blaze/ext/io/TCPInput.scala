package gdg.blaze.ext.io

import java.net.ServerSocket
import java.util.concurrent.ExecutorService

import gdg.blaze._
import gdg.blaze.codec.JSONCodec
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

import scala.collection.mutable

class TCPInput(pc: PluginConfig, bc: BlazeContext, source: SourceInput) extends Input {

  override def apply(): DStream[Message] = {
    val mode = pc.getString("mode").getOrElse("server")
    mode match {
      case "server" => server()
      case "client" => throw new IllegalStateException("Client mode not yet supported")
    }
  }

  def server(): DStream[Message] = {
    val codec = pc.getCodec("codec")(bc).getOrElse(new JSONCodec())
    val port = pc.getInt("port").getOrElse(4000)
    val ss = new ServerSocket(port)
    val socket = ss.accept()
    val stream = scala.io.Source.fromInputStream(socket.getInputStream)
    source(stream).flatMap(codec.decode)
  }
}

object TCPInput extends PluginFactory[TCPInput] {
  override def apply(pc: PluginConfig, bc: BlazeContext): TCPInput = new TCPInput(pc, bc, new SourceInput(pc, bc))
}

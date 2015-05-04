package gdg.blaze.ext.hadoop

import gdg.blaze._
import gdg.blaze.codec.{PlainCodec, JSONCodec}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.streaming.dstream.{DStream, InputDStream}


/* add_field => ... # hash (optional), default: {}
    codec => ... # codec (optional), default: "plain"
    discover_interval => ... # number (optional), default: 15
    exclude => ... # array (optional)
    path => ... # array (required)
    sincedb_path => ... # string (optional)
    sincedb_write_interval => ... # number (optional), default: 15
    start_position => ... # string, one of ["beginning", "end"] (optional), default: "end"
    stat_interval => ... # number (optional), default: 1
    tags => ... # array (optional)
    type => ... # string (optional)
  format = (text, or binary, etc)
 */
class HdfsInput(config: HdfsConfig, @transient bc: BlazeContext) extends Input {

  override def apply(): DStream[Message] = {
    if(config.path.isEmpty) {
      throw new IllegalStateException("hdfs plugin requires path field")
    }
    val mstream = config.format match {
      case "text" =>text(config.path, newFiles = config.new_files_only)
    }
    mstream.flatMap { str => config.codec.decode(str)}
  }

  def text(path: String, newFiles: Boolean): DStream[String] = {
    val stream: InputDStream[(LongWritable, Text)] = bc.sc.fileStream[LongWritable, Text, TextInputFormat](path, Function.const(true) _, newFiles)
    stream.print()
    stream.map(_._2.toString)
  }
}
case class HdfsConfig(
                  format:String = "text",
                  path:String,
                  codec:Codec = PlainCodec.single,
                  new_files_only:Boolean = false)
object HdfsInput extends PluginFactory[HdfsInput] {
  override def apply(config: PluginConfig, bc: BlazeContext) = {
    new HdfsInput(config.convert(classOf[HdfsConfig]), bc)
  }
}
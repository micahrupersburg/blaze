package gdg.blaze


import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.io.Source

object BlazeOfGlory {
  type MStream = DStream[Message]
  val registry = Registry

  def loadFile(resource: String) = {
    Source.fromURL(getClass.getResource(resource)).mkString
  }

  def main(args: Array[String]): Unit = {
    val sc = run("/demo.conf").sc
    sc.start()
    sc.awaitTermination()
  }

  def run(file: String): BlazeContext = {
    val conf: SparkConf = new SparkConf().setAppName("Hadoop DB TX Loader").setMaster("local[4]")
    val bc = new BlazeContext(new StreamingContext(conf, Seconds(1)))
    println(s"Loading : $file")
    val exp = loadFile(file)
      .replaceAll("#.*\n", "")
    val parseResult = new ConfigParser().config(exp)
    if(!parseResult.successful) {
      throw new IllegalStateException(parseResult.toString)
    }
    val parsed = parseResult.get
    val outputs = parsed.output.map { case b: NamedObjectValue => Registry.output(b, bc) }.map(validate)
    val inputs = parsed.input.map { case b: NamedObjectValue => Registry.input(b, bc) }.map(validate)
    val filters = parsed.output.map { case b: NamedObjectValue => Registry.filter(b, bc) }.map(validate)
    val config = PluginConfig(parsed.config)
    val stream: DStream[Message] = bc.sc.union(inputs.map(_()).toSeq)
    val f = filteredStream(stream, filters)
//    outputs.foreach {
//      case x:Output => x.process(filteredStream())
//    }
    bc
  }

  def filteredStream(dStream: DStream[Message], inputs:Traversable[Filter]): DStream[Message] = {
    var s = dStream
    for(f <- inputs) {
      s = f(s)
    }
    s
  }

  def validate[X <: Plugin](v: Option[X]): X = {
    if(v.isEmpty) {
      throw new IllegalStateException()
    }
    v.get
  }

  def pred: Conditional => Message => Boolean = {
    case c => message => true
  }
}

package gdg.blaze


import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.io.Source

object Main {
  type MStream = DStream[Message]
  val registry = Registry

  def loadFile(resource: String) = {
    Source.fromURL(getClass.getResource(resource)).mkString
  }

  def main(args: Array[String]): Unit = {
    args.foreach { file =>
      val sc = run(file).sc
      sc.start()
      sc.awaitTermination()
    }
  }

  def run(file: String): BlazeContext = {
    val conf: SparkConf = new SparkConf().setAppName("Hadoop DB TX Loader").setMaster("local[4]")

    println(s"Loading : $file")
    val exp = loadFile(file)
    val parseResult = new ConfigParser().config(exp)
    if(!parseResult.successful) {
      throw new IllegalStateException(parseResult.toString)
    }
    val parsed = parseResult.get
    println(parsed)
    val bc = new BlazeContext(new StreamingContext(conf, Seconds(5)))

    val outputs = parsed.output.map { case b: NamedObjectValue => Registry.output(b, bc) }
    val inputs = parsed.input.map { case b: NamedObjectValue => Registry.input(b, bc) }
    val filters = parsed.filter.map { case b: NamedObjectValue => Registry.filter(b, bc) }
    val config = PluginConfig(parsed.config)
    if(inputs.isEmpty) {
      throw new IllegalStateException("No Inputs Given")
    }
    val stream: DStream[Message] = bc.sc.union(inputs.map(_()).toSeq)
    val f = filteredStream(stream, filters)
    for(o <- outputs) {
      o(f)
    }
    bc
  }

  def filteredStream(dStream: DStream[Message], inputs:Traversable[Filter]): DStream[Message] = {
    var s = dStream
    for(f <- inputs) {
      s = f(s)
    }
    s
  }


  def pred: Conditional => Message => Boolean = {
    case c => message => true
  }
}

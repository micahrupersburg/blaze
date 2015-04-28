package gdg.blaze


import gdg.blaze.ext.{StdOut, GrokFilter, HdfsInput}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.io.Source

object BlazeOfGlory {
  def filterPlugins: List[PluginFactory[_ <: FilterPlugin]] = List(GrokFilter)

  def inputPlugins: List[PluginFactory[_ <: InputPlugin]] = List(HdfsInput)

  def outputPlugins: List[PluginFactory[_ <: OutputPlugin]] = List(StdOut)

  val inputMap: Map[String, PluginFactory[_ <: InputPlugin]] = inputPlugins.map { p =>
    (p.name, p)
  }.toMap
  val outputMap: Map[String, PluginFactory[_ <: OutputPlugin]] = outputPlugins.map { p =>
    (p.name, p)
  }.toMap

  def exec(inputs: List[InputPlugin], filters: List[FilterPlugin], outputs: List[OutputPlugin]): StreamingContext = {
    val conf: SparkConf = new SparkConf().setAppName("Hadoop DB TX Loader").setMaster("local[4]")
    val ssc = new StreamingContext(conf, Seconds(1))
    val fdd = inputs.map(_.create(ssc))
    fdd.foreach { xs =>
      outputs.foreach { out =>
        out.process(xs)
      }
    }
    ssc
  }

  def loadFile(resource: String) = {
    Source.fromURL(getClass.getResource(resource)).mkString
  }

  def main(args: Array[String]): Unit = {
    val sc = run("/demo.conf")
    sc.start()
    sc.awaitTermination()
  }

  def run(file: String): StreamingContext = {
    println(s"Loading : $file")
    val exp = loadFile(file)
      .replaceAll("#.*\n", "")
    val config: ConfigParser#ParseResult[EntireConfig] = new ConfigParser().config(exp)
    println(config)

    exec(config.get.input.flatMap(inputPlugin), List.empty, config.get.output.flatMap(outputPlugin))
  }

  def inputPlugin(b: Body): Traversable[InputPlugin] = {
    b match {
      case b: NamedObjectValue if inputMap.contains(b.name) => Some(inputMap.get(b.name).get.create(new PluginConfig(b.members)))
      case b: NamedObjectValue if !inputMap.contains(b.name) => throw new IllegalArgumentException(s"No such input type : ${b.name}")
      case _ => throw new IllegalArgumentException("Conditionals not supported in input block")
    }
  }

  def outputPlugin(body: Body): Traversable[OutputPlugin] = {
    body match {
      case b: NamedObjectValue if outputMap.contains(b.name) => Some(outputMap.get(b.name).get.create(new PluginConfig(b.members)))
      case b: NamedObjectValue if !outputMap.contains(b.name) => throw new IllegalArgumentException(s"No such output type : ${b.name}")
      //      case ic: IfCond => ic.body.flatMap(outputPlugin).map(new FilteredOutputPlugin(_, ))
      case _ => throw new IllegalArgumentException("Conditionals Not Yet Supported")
    }
  }

  def pred(conditional: Conditional): (Message) => Boolean = {
    conditional match {
      case c => message => true
    }
  }

}


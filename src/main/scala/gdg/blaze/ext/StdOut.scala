package gdg.blaze.ext

import gdg.blaze.{Message, BaseOutput, PluginConfig, PluginFactory}
import org.apache.spark.streaming.dstream.DStream

class StdOut(config: PluginConfig) extends BaseOutput(config) {
  override def process(dStream: DStream[Message]) = {
    dStream.foreachRDD { rdd =>
      if(!rdd.isEmpty()) {
        rdd.collect().foreach(println)
      }
    }
  }
}

object StdOut extends PluginFactory[StdOut]{
  override def create(config: PluginConfig): StdOut = new StdOut(config)

  override def name: String = "stdout"
}

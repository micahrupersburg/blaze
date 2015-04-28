package gdg.blaze

import java.util.concurrent.{LinkedBlockingQueue, BlockingQueue}

import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

trait Plugin extends Serializable {

}

trait InputPlugin extends Plugin {
  def create(sc: StreamingContext): DStream[Message]
}

trait OutputPlugin extends Plugin {
  def process(dStream: DStream[Message])
}

trait FilterPlugin extends Plugin {

}

class BasePlugin(config: PluginConfig) extends Plugin with LifeCycle {
  override def start: Unit = ???

  override def stop: Unit = ???
}

class BaseFilter(config: PluginConfig) extends BasePlugin(config) with FilterPlugin {

}

trait LifeCycle {
  def start
  def stop
}

abstract class BaseInput(val config: PluginConfig) extends BasePlugin(config) with InputPlugin {
  def stream(sc: StreamingContext): DStream[Message]

  override final def create(sc: StreamingContext): DStream[Message] = {
    stream(sc).map(process)
  }

  final def process(msg: Message): Message = {
    msg
  }
}

abstract class BaseOutput(config: PluginConfig) extends BasePlugin(config) with OutputPlugin {
}

trait PluginFactory[T <: Plugin] {
  def name: String

  def create(config: PluginConfig): T
}

class PluginConfig(val config: Map[String, Value]) extends Serializable {
  def getInt(name: String): Option[Int] = {
    getNum(name).map(_.toInt)
  }

  def getNum(name: String): Option[Double] = {
    config.get(name).map {
      case x: SingleFloat => x.value
      case _ => throw new IllegalStateException()
    }
  }

  def getString(name: String): Option[String] = {
    config.get(name).map {
      case x: SingleString => x.value
      case _ => throw new IllegalStateException()
    }
  }

  def getInterString(name: String): Message => Option[String] = {
    config.get(name).map {
      case x: SingleString => _: Message => Some(x.value)
      case _ => throw new IllegalStateException()
    }
  }

  def getBool(name: String): Option[Boolean] = {
    config.get(name).map {
      case x: SingleBool => x.value
      case x: SingleString => x.value.toBoolean
      case _ => throw new IllegalStateException()
    }
  }
}

class FilteredOutputPlugin(other: OutputPlugin, predicate: (Message) => Boolean) extends OutputPlugin {
  override def process(dStream: DStream[Message]): Unit = {
    other.process(dStream.filter(predicate))
  }
}

trait Codec[T] extends Plugin {
  def transform(item: T): Traversable[Message]
}

trait StringCodec extends Codec[String]
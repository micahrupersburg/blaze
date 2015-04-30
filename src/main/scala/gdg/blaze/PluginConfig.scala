package gdg.blaze


import scala.collection.mutable

object PluginConfig extends(Traversable[Member]=> PluginConfig) {
  def empty = new PluginConfig()

  override def apply(value: Traversable[Member]) :PluginConfig = new PluginConfig(value)
  //  override def apply(value: Traversable[Member]): PluginConfig =

  def filter(value: Traversable[Member]): PluginConfig = {
    new PluginConfig(value)
  }
}

class InterString(value:String) extends (Message => String) {
  override def apply(v1: Message): String = ???
}

object Converter extends (Traversable[Member] => mutable.Map[String, List[Any]]) {

  def apply(members:Traversable[Member]) : mutable.Map[String, List[Any]] = {
    val mm = new mutable.HashMap[String, List[Any]]
    for(m <- members) {
      val l = mm.get(m.key)
      val x = convert(m.value)
      if(!l.isDefined) {
        mm.put(m.key, List(x))
      } else {
        l.get.+:(x)
      }
    }
    mm
  }

  def convert: Value => Any = {
    case v:SingleBool => v.value
    case v:SingleFloat => v.value
    case v:SingleString => v.value
    case v:ArrayValue => v.value.map(convert)
    case v:ObjectValue => this(v.value)
    case v:NamedObjectValue => (v.name, this(v.value))
  }
}

class PluginConfig(members: Traversable[Member] = None) extends Serializable {

  // to create a `MultiMap` the easiest way is to mixin it into a normal
  // `Map` instance

  val config = Converter(members)

  def getInt(name: String): Option[Int] = {
    getNum(name).map(_.toInt)
  }

  def one(name:String) : Option[Any] = {
    val v = config.get(name)
    if(v.isDefined)
      Some(v.get.head)
    else
      None
  }

  def many[X](name:String) : Traversable[X] = {
    val v = config.get(name)
    if(v.isDefined)
      v.get.map(_.asInstanceOf[X])
    else
      None
  }

  def getCodec(name: String)(bc: BlazeContext): Option[Codec] = {
    one(name).map {
      case x: String => Registry.codec(new NamedObjectValue(x), bc)
      case _ => throw new IllegalStateException()
    }
  }

  def getNum(name: String): Option[Double] = {
    one(name).map {
      case x: Number => x.doubleValue()
      case _ => throw new IllegalStateException()
    }
  }

  def getString(name: String): Option[String] = {
    one(name).map {
      case x: String => x
      case _ => throw new IllegalStateException()
    }
  }

  def getInterString(name: String): Message => Option[String] = {
    val v = one(name)
    if(v.isDefined) {
      v.get match {
        case x: String => _: Message => Some(x)
        case _ => throw new IllegalStateException()
      }
    } else {
      Message => None
    }
  }

  def getBool(name: String): Option[Boolean] = {
    one(name).map {
      case x: Boolean => x
      case x: String => x.toString.toBoolean
      case _ => throw new IllegalStateException()
    }
  }
}
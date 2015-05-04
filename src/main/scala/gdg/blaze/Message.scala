package gdg.blaze

import java.util.UUID

class Message(var fields: Map[String, _] = Map.empty,
              var message:String = null,
              var source: String = "unknown",
              var typeParam: String = "unknown",
              var tags: Set[String] = Set.empty,
              var timestamp: Long = System.currentTimeMillis(),
              val uuid: UUID = UUID.randomUUID()
               ) extends Serializable {
  def addTags(tags: Set[String]) = {
    this.tags = this.tags ++ tags
  }
  def set(name:String, value:Any) = {this.fields = fields++ Map(name -> value)}
  def getString(name: String): Option[String] = get(name).map(_.asInstanceOf[String])

  def get(name: String): Option[Any] = fields.get(name)

  override def toString = s"Message($fields, $message, $source, $typeParam, $tags, $timestamp, $uuid)"
}

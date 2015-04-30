package gdg.blaze

import java.util.UUID

class Message(
               var tags: Set[String] = Set.empty,
               var data: Map[String, _] = Map.empty,
               var ts: Long = System.currentTimeMillis(),
               val uuid: UUID = UUID.randomUUID()
               ) extends Serializable {
  def addTags(tags: Set[String]) = {
    this.tags = this.tags ++ tags
  }

  def getString(name: String): Option[String] = data.get(name).map(_.asInstanceOf[String])
}

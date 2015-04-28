package gdg.blaze

class Message(
               var tags:Set[String] = Set.empty,
               var data:Map[String, _] = Map.empty,
               var ts:Long = System.currentTimeMillis()
               ) extends Serializable{
  def addTags(tags:Set[String]) = {this.tags = this.tags ++ tags}
}

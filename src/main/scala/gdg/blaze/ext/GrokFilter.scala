package gdg.blaze.ext

import com.fasterxml.jackson.annotation.JsonProperty
import com.google.common.base.Charsets
import com.google.common.io.Resources
import gdg.blaze._
import oi.thekraken.grok.api.{Match, Grok}


class GrokFilter(config: GrokFilterConfig, val filter: MessageFilter) extends BasicFilter {
  @transient lazy val grok: Grok = {
    val g: Grok = new Grok()
    config.patterns_dir.foreach{ file =>
      val gpat = file.replaceFirst("^\\./", "")
      g.addPatternFromReader(Resources.asCharSource(Resources.getResource(gpat), Charsets.UTF_8).openBufferedStream())
    }
    config.matches.foreach{
      case (x:String, y:List[String]) => y.foreach(g.compile)
      case (x:String, y:String) => g.compile(y)
    }
    g
  }

  override def transform(msg: Message): Traversable[Message] = {
    import collection.JavaConverters._
    config.matches.foreach { mx =>
      val field: Option[Any] = msg.get(mx._1)
      if(field.isDefined) {
        val gmat: Match = grok.`match`(field.get.toString)
        gmat.captures()
        msg.fields = msg.fields ++ gmat.toMap.asScala
      }
    }
    Some(msg)
  }

}

case class GrokFilterConfig(
                             @JsonProperty("match") matches: Map[String, _],
                             patterns_dir:List[String] = List("grok-patterns")
                             )

/*  add_field => ... # hash (optional), default: {}
add_tag => ... # array (optional), default: []
break_on_match => ... # boolean (optional), default: true
drop_if_match => ... # boolean (optional), default: false
keep_empty_captures => ... # boolean (optional), default: false
match => ... # hash (optional), default: {}
named_captures_only => ... # boolean (optional), default: true
overwrite => ... # array (optional), default: []
patterns_dir => ... # array (optional), default: []
remove_field => ... # array (optional), default: []
remove_tag => ... # array (optional), default: []
tag_on_failure => ... # array (optional), default: ["_grokparsefailure"]*/
object GrokFilter extends PluginFactory[GrokFilter] {

  def matches(config: PluginConfig): Map[String, List[String]] = {
    val option: Option[Any] = config.get("match")
    if(option.isEmpty) {
      Map()
    } else {
      val xx = option.get match {
        case m: List[List[String]] => m.map { x => (x(0), x(1)) }.groupBy(_._1).mapValues(_.map(_._2))
        case m: List[String] => m.sliding(2, 2).map { x => (x(0), x(1)) }.toMap.mapValues(List(_))
        case m: Map[String, String] => m.mapValues(List(_))
      }
      Map(xx.toSeq: _*)
    }

  }

  override def apply(config: PluginConfig, bc: BlazeContext) = new GrokFilter(new GrokFilterConfig(matches(config)), bc.filter)
}

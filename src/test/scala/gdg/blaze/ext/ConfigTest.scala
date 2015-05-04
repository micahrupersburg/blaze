package gdg.blaze.ext

import com.fasterxml.jackson.databind.ObjectMapper
import gdg.blaze.PluginConfig
import org.testng.annotations.Test

class ConfigTest {

  @Test
  def test = {
    println(new PluginConfig(Map("l" -> "21234")).convert(classOf[ListTest]))
    println(new PluginConfig(Map("l" -> List(List("abc", "123"), List("xyz", "457")))).convert(classOf[MapTest]))
  }
}

case class ListTest(l:List[String])
case class MapTest(l:Map[String,_])

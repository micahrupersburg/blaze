package gdg.blaze.ext

import gdg.blaze.{Message, MessageFilter}
import org.testng.annotations.Test

class GeoIpFilterTest {
  @Test
  def test = {
    val msgs = new GeoIpFilter(new GeoIpConfig("ip"), new MessageFilter())
      .transform(new Message(Map("ip" -> "128.101.101.101")))
    println(msgs)
  }
}

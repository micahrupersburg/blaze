package gdg.blaze.ext

import com.google.common.base.Charsets
import com.google.common.io.Resources
import gdg.blaze._
import org.testng.annotations.Test


class TestGrokFilter {
  @Test
  def test: Unit = {
    val log = "112.169.19.192 - - [06/Mar/2013:01:36:30 +0900] \"GET / HTTP/1.1\" 200 44346 \"-\" \"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_2) AppleWebKit/537.22 (KHTML, like Gecko) Chrome/25.0.1364.152 Safari/537.22\""
    Main.main(Array("/gdg/blaze/ext/grok.conf"))
    val grok = new GrokFilter(new GrokFilterConfig(Map("k" -> List("%{COMBINEDAPACHELOG}"))), new MessageFilter())
    val transform: Traversable[Message] = grok.transform(new Message(Map("k" -> log)))
    println(transform)
  }
}

package gdg.blaze.godaddy

import com.google.common.io.Resources
import com.google.common.net.HostAndPort
import gdg.blaze._
import gdg.feed.netty.FeedSpec
import gdg.feed.proto.gen.Feed.Envelope
import gdg.feed.proto.gen.Feed.ServiceHeader.Subscribe.Offsets
import gdg.feed.proto.gen.Feed.ServiceHeader.Subscribe.StartingPoint.Edge
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream

class FeedInput(pc: PluginConfig, bc: BlazeContext) extends Input {
  override def apply(): DStream[Message] = {
    val feed = pc.getString("feed")
    val spec = FeedSpec.builder()
      .`type`(Envelope.getDefaultInstance).websocket()
      .sub()
      .feed(feed.get)
      .start(Edge.LATEST, Offsets.getDefaultInstance)
      .protobuf()
      .location(HostAndPort.fromParts("feed-http.databus.prod.int.godaddy.com", 443))
    val keystore = Resources.asByteSource(Resources.getResource("parquet-feed-writer-prod.jks")).read()
    new FeedClientDStream(bc.sc, StorageLevel.MEMORY_AND_DISK, spec.build().toString, keystore, "parquet-feed-writer-password")
  }
}


object FeedInput extends PluginFactory[FeedInput] {
  override def apply(pc: PluginConfig, bc: BlazeContext): FeedInput = new FeedInput(pc, bc)
}

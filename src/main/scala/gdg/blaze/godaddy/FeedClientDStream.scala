package gdg.blaze.godaddy

import java.io.{ByteArrayInputStream, File}
import java.security.{SecureRandom, KeyStore}
import java.util

import com.google.common.io.Resources
import gdg.blaze.Message
import gdg.blaze.codec.JSONCodec
import gdg.feed.client.FeedClient
import gdg.feed.netty.FeedSpec
import javax.net.ssl.{KeyManagerFactory, TrustManagerFactory, SSLContext}
import gdg.feed.proto.TimeHelper
import gdg.feed.proto.gen.DCR.RawEvent
import gdg.feed.proto.gen.Feed.{Offset, Event, Envelope}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream._
import gdg.feed.proto.gen.Feed
import gdg.feed.util.CodedException
import java.util.concurrent.TimeUnit
import org.apache.spark.streaming.receiver.Receiver

class FeedClientDStream(ssc: StreamingContext,
                        storageLevel: StorageLevel,
                        feedSpec: String,
                        keyStore: Array[Byte],
                       keyStorePassword:String
                         )
  extends ReceiverInputDStream[Message](ssc) {

  override def getReceiver(): Receiver[Message] = {
    new FeedClientReceiver(storageLevel, feedSpec, keyStore,keyStorePassword)
  }
}

class FeedClientReceiver(storageLevel: StorageLevel, feedSpec: String, keyStoreContent: Array[Byte], keyStorePassword:String) extends Receiver[Message](storageLevel) {

  def client = FeedClient.subscribe(FeedSpec.parse(feedSpec), proxy, sslContext())

  def sslContext() = {
    val password = keyStorePassword.toCharArray
    val keyStore = {
      val ks = KeyStore.getInstance("JKS")
      ks.load(new ByteArrayInputStream(keyStoreContent), password)
      ks
    }

    val trustManagers = {
      val tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
      tmf.init(null: KeyStore)
      tmf.getTrustManagers
    }

    val keyManagers = {
      val kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm)
      kmf.init(keyStore, password)
      kmf.getKeyManagers
    }

    val sslContext = SSLContext.getInstance("TLS")
    sslContext.init(keyManagers, trustManagers, null: SecureRandom)
    sslContext
  }

  private object proxy extends FeedClient.FeedSubscriber[Feed.Envelope] {
    override def protoType(): Envelope = {
      Envelope.getDefaultInstance
    }

    override def close(): Unit = {
      println("CLOSING")
    }

    override def received(value: Envelope, list: util.List[Offset]): Unit = {
      FeedClientReceiver.this.store(convert(value).toIterator)
    }

    val json = new JSONCodec()
    import collection.JavaConverters._
    def convert(envelope: Envelope): Traversable[Message] = {
      if( (!envelope.hasHeader) || (!envelope.getHeader.hasQualifiedMessageType) || envelope.getHeader.getQualifiedMessageType.equalsIgnoreCase(RawEvent.getDescriptor.getName)) {
        val time = TimeHelper.cal(envelope.getTimestamp).getMillis
        val itemsList: util.List[Event] = envelope.getTransaction.getItemsList
        itemsList.asScala.flatMap { e =>
          val re = RawEvent.parseFrom(e.getPayload)
          val jout = json.decode(re.getJson)
          jout.foreach{m =>
            m.timestamp = time
          }
          jout
        }
      } else {
        None
      }
    }

    override def handleError(response: Feed.Response): Unit = {
      val exc = new CodedException(response)

      if(response.getCode != 795) {
        FeedClientReceiver.this.stop(s"Error From Server : $response", exc)
      }
    }

  }

  override def onStart(): Unit = {
    client.start(1, TimeUnit.MINUTES)
  }

  override def onStop(): Unit = {
    client.close()
  }
}
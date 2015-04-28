package gdg.blaze.ext

import java.util.concurrent.TimeUnit

import com.google.common.base.Stopwatch
import gdg.blaze.{PluginConfig, Message, BaseOutput}
import org.apache.spark.streaming.dstream.DStream
import org.elasticsearch.action.ListenableActionFuture
import org.elasticsearch.action.bulk.{BulkResponse, BulkRequestBuilder, BulkRequest}
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.common.transport.{InetSocketTransportAddress, TransportAddress}

class ElasticSearchOutput(config: PluginConfig) extends BaseOutput(config) {
  val action = config.getString("action").getOrElse("index")
  //  val bind_host = config.getString("bind_host")
  //  val bind_port  = config.getString("bind_port")
  //  val cluster = config.getString("cluster")
  val host = config.getString("host")
  val idle_flush_time = config.getInt("idle_flush_time").getOrElse(1)
  val manage_template = config.getBool("manage_template").getOrElse(true)
  val node_name = config.getString("node_name")
  val protocol = config.getString("protocol").getOrElse("transport")
  val index = config.getInterString("index")
  val index_type = config.getInterString("index_type")

  //  val cluster = config.getString("cluster")
  def defaultPort(protocol: String): Int = {
    case "transport" => 9300
    case "http" => 9200
  }

  val port = config.getInt("port").getOrElse(defaultPort(protocol))
  val timer = Stopwatch.createUnstarted()
  var tc: TransportClient

  override def start: Unit = {
    if(!host.isDefined) {
      throw new IllegalStateException("Undefined Config Parameter : host")
    }
    tc = createClient()
  }

  override def stop: Unit = {
    tc.close()
  }
  class BulkSender {

  }
  var bulk: Option[BulkRequestBuilder] = None
  def sendBulkIfNecessary = {
    if(bulk.isDefined && timer.elapsed(TimeUnit.SECONDS) > idle_flush_time) {
      timer.reset().start()
      val bulkItemResponses: BulkResponse = bulk.get.execute().actionGet(60, TimeUnit.SECONDS)

      if(bulkItemResponses.hasFailures) {
        throw new IllegalStateException("Bulk Failure : " + bulkItemResponses.buildFailureMessage())
      }
    }
  }
  override def process(dStream: DStream[Message]): Unit = {

    dStream.foreachRDD { rdd =>
      sendBulkIfNecessary
    }

  }

  private[search] def createClient(): TransportClient = {
    val settings = ImmutableSettings.builder
      .put("client.transport.sniff", "false")
      .put("client.transport.ignore_cluster_name", "true")
      .build

    val tc = new TransportClient(settings, false)
    host.get.split(",").foreach { h =>
      return tc.addTransportAddress(new InetSocketTransportAddress(h.trim, port))
    }
    tc
  }

}

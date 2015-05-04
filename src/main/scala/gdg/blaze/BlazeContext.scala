package gdg.blaze

import org.apache.spark.streaming.StreamingContext

class BlazeContext(@transient val sc:StreamingContext, val filter:MessageFilter = new MessageFilter()) extends Serializable

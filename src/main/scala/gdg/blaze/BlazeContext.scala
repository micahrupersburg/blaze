package gdg.blaze

import org.apache.spark.streaming.StreamingContext

class BlazeContext(val sc:StreamingContext, val filter:MessageFilter = new MessageFilter())

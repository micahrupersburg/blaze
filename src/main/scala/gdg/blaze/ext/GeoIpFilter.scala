package gdg.blaze.ext

import java.net.InetAddress

import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.google.common.io.Resources
import com.maxmind.geoip2.DatabaseReader
import com.maxmind.geoip2.record.AbstractNamedRecord
import gdg.blaze._

class GeoIpFilter(config: GeoIpConfig, val filter: MessageFilter) extends BasicFilter {
  lazy val mapper = {
    val m = new ObjectMapper()
    m.registerModule(DefaultScalaModule)
    m.setSerializationInclusion(Include.NON_NULL)
    m
  }

  override def transform(msg: Message): Traversable[Message] = {
    val file = Resources.asByteSource(Resources.getResource(config.database)).openStream()
    val reader = new DatabaseReader.Builder(file).build()
    val ipAddress = InetAddress.getByName("128.101.101.101")
    val city = reader.city(ipAddress)
    def transform(prefix: String)(value: (String, _)) = (prefix + value._1, value._2)
    def nameId(prefix: String, rec: AbstractNamedRecord): Map[String, _] = Map(prefix + "name" -> rec.getName, prefix + "code" -> rec.getGeoNameId)

    val geoip: Map[String, _] =
      nameId("city_", city.getCity) ++
        nameId("continent_", city.getContinent) ++
        nameId("country_", city.getCountry) ++
        Map("postal_code" -> city.getPostal.getCode) ++
        mapper.convertValue(city.getLocation, classOf[Map[String, _]])
    msg.fields = msg.fields ++ geoip
    Some(msg)
  }

}

case class GeoIpConfig(
                        source: String,
                        target: String = "geoip",
                        database: String = "GeoLite2-City.mmdb",
                        fields: List[String] = null //List("city\\_name", "continent\\_code", "country\\_code2", "country\\_code3", "country\\_name", "dma\\_code", "ip", "latitude", "longitude", "postal\\_code", "region\\_name and timezone")
                        )

object GeoIpFilter extends PluginFactory[GeoIpFilter] {
  override def apply(pc: PluginConfig, bc: BlazeContext): GeoIpFilter = new GeoIpFilter(pc.convert(classOf[GeoIpConfig]), bc.filter)
}

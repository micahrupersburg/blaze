package gdg.blaze.ext

import gdg.blaze.{BaseFilter, PluginConfig, BasePlugin, PluginFactory}


class GrokFilter(config: PluginConfig) extends BaseFilter(config) {

}

object GrokFilter extends PluginFactory[GrokFilter] {
  override def name = "grok"
  override def create(config: PluginConfig) = new GrokFilter(config)
}

package sts.kafka.sink

import spock.lang.Specification
import org.apache.kafka.connect.sink.SinkConnector
import sts.kafka.sink.StsSinkConnector

class StsSinkConnectorTest extends Specification {
  def ".version() should return the latest"() {
    setup:
    def sink = new StsSinkConnector()

    when:
    def v = sink.version()

    then:
    v == "0.0.1"
  }

  def ".config() should return a valid config definition"() {
    setup:
    def sink = new StsSinkConnector()

    when:
    def configDefs = sink.config()

    then:
    configDefs.configKeys().containsKey(Config.OUTPUT_FILE)
  }

  def ".taskConfigs() should generate valid task configs"() {
    setup:
    def count = 5
    def filename = "some-file.log"
    def reporter = "some-reporter"
    def props = [:]
    props[Config.OUTPUT_FILE] = filename
    props[Config.REPORTER] = reporter
    def sink = new StsSinkConnector()

    when:
    sink.start(props)
    def configs = sink.taskConfigs(count)

    then:
    configs.size() == count
    configs.stream().filter({c -> c.containsKey(Config.OUTPUT_FILE)}).count() == count
    configs.stream().filter({c -> c.containsKey(Config.REPORTER)}).count() == count
    configs.stream().filter({c -> c.get(Config.OUTPUT_FILE) == filename}).count() == count
    configs.stream().filter({c -> c.get(Config.REPORTER) == reporter}).count() == count
  }
}

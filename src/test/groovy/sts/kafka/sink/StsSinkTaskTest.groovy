package sts.kafka.sink

import java.nio.file.Files

import spock.lang.Specification
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.data.Schema

class StsSinkTaskTest extends Specification {
  def ".version() should return the latest"() {
    setup:
    def task = new StsSinkTask()

    when:
    def v = task.version()

    then:
    v == "0.0.1"
  }

  def ".put() should return a valid config definition"() {
    setup:
    // Reporter log file
    def tmpFile = Files.createTempFile("test", ".log")
    def filename = tmpFile.toString()
    def reporter = "StsFileReporter"
    def props = [:]
    props[Config.OUTPUT_FILE] = filename
    props[Config.REPORTER] = reporter

    // Some kafka events
    def topic = "some-topic"
    def partition = 5
    def offset = 23
    def keySchema = Schema.STRING_SCHEMA
    def valueSchema = Schema.STRING_SCHEMA
    def key1 = "test-key-1"
    def value1 = "test-value-1"
    def record1 = new SinkRecord(topic, partition, keySchema, key1, valueSchema, value1, offset)
    def key2 = "test-key-2"
    def value2 = "test-value-2"
    def record2 = new SinkRecord(topic, partition, keySchema, key2, valueSchema, value2, offset)
    def records = [record1, record2]

    // The sink task
    def task = new StsSinkTask()

    when:
    task.start(props)
    task.put(records)

    then:
    new String(Files.readAllBytes(tmpFile)) == value1 + value2
  }
}

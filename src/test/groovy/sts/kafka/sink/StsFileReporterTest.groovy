package sts.kafka.sink

import java.nio.file.Files

import spock.lang.Specification
import org.apache.kafka.connect.sink.SinkConnector
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.data.Schema

class StsFileReporterTest extends Specification {
  def ".write() should write the record to a file"() {
    setup:
    def topic = "some-topic"
    def partition = 5
    def offset = 23
    def keySchema = Schema.STRING_SCHEMA
    def valueSchema = Schema.STRING_SCHEMA
    def key = "test-key"
    def value = "test-value"
    def record = new SinkRecord(topic, partition, keySchema, key, valueSchema, value, offset)
    def tmpFile = Files.createTempFile("test", ".log")

    when:
    def reporter = new StsFileReporter(tmpFile.toString())
    reporter.write(record)

    then:
    new String(Files.readAllBytes(tmpFile)) == value
  }
}

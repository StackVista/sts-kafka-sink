package sts.kafka.sink;

import java.util.Map;

import org.apache.kafka.connect.sink.SinkRecord;


public abstract class StsReporter {

  public static StsReporter get(Map<String, String> props) {
    String reporter = props.get(Config.REPORTER);

    switch (reporter) {
      // TODO Add a reporter that makes more sense.
      case "StsFileReporter":
      default:
        String outputFile = props.get(Config.OUTPUT_FILE);
        return new StsFileReporter(outputFile);
    }
  }

  public void write(SinkRecord record) {}

}

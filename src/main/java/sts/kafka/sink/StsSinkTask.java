package sts.kafka.sink;

import java.util.Map;
import java.util.Collection;

import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;


public class StsSinkTask extends SinkTask {
  public static String VERSION = "0.0.1";

  private String outputFile = "";

  @Override
  public void start(Map<String, String> props) {
    this.outputFile = props.get(Config.OUTPUT_FILE);
  }

  @Override
  public void stop() {
    // Nothing to do.
  }

  @Override
  public String version() {
    return VERSION;
  }

  @Override
  public void put(Collection<SinkRecord> records) {
    // TODO Dump records to outputFile.
  }

}

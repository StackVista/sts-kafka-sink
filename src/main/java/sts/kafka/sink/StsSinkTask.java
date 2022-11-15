package sts.kafka.sink;

import java.util.Map;
import java.util.Collection;

import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;


public class StsSinkTask extends SinkTask {
  public static String VERSION = "0.0.1";

  private StsReporter reporter;

  @Override
  public void start(Map<String, String> props) {
    this.reporter = StsReporter.get(props);
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
    records.forEach((r) -> this.reporter.write(r));
  }

}

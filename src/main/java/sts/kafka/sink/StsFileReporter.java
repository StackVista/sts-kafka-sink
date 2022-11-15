package sts.kafka.sink;

import java.util.Map;
import java.io.FileOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.kafka.connect.sink.SinkRecord;


public class StsFileReporter extends StsReporter {
  private FileOutputStream outputFile;

  public StsFileReporter(String filename) {
    try {
      this.outputFile = new FileOutputStream(filename, true);
    } catch (FileNotFoundException e) {
      // Welp, just die and burn I suppose.
    }
  }

  protected void finalize() {
    try {
      this.outputFile.close();
    } catch (IOException e) {
      // No harm done.
    }
  }

  @Override
  public void write(SinkRecord record) {
    // TODO Deserialize to an OTEL metric model and properly use it.
    try {
      this.outputFile.write(record.value().toString().getBytes());
    } catch (IOException e) {
      // Just silently skip it for extra fun debugging later on.
    }
  }

}

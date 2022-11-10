package sts.kafka.sink;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;


public class StsSinkConnector extends SinkConnector {
  public static String VERSION = "0.0.1";
  private String outputFile = "";

  @Override
  public ConfigDef config() {
    return new ConfigDef().define(Config.OUTPUT_FILE, ConfigDef.Type.STRING, Importance.HIGH, "Output log file.");
  }

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
  public Class<? extends Task> taskClass() {
    return StsSinkTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    return IntStream.range(0, maxTasks).boxed().map((i) -> this.taskConfig()).collect(Collectors.toList());
  }

  private Map<String, String> taskConfig() {
    Map<String, String> conf = new HashMap<String, String>();
    conf.put(Config.OUTPUT_FILE, this.outputFile);
    return conf;
  }

}

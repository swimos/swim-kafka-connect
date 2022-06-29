// Copyright 2015-present SWIM.AI inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package swim.kafka.connector.sink;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

public class SwimKafkaSinkConnector extends SinkConnector {

  private Map<String, String> props;

  @Override
  public Class<? extends Task> taskClass() {
    return SwimKafkaSinkTask.class;
  }

  @Override
  public void start(Map<String, String> props) {
    this.props = props;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    final List<Map<String, String>> configs = new ArrayList<>(maxTasks);
    for (int i = 0; i < maxTasks; ++i) {
      configs.add(this.props);
    }
    return configs;
  }

  @Override
  public void stop() {

  }

  @Override
  public ConfigDef config() {
    return SwimSinkConfig.SWIM_SINK_CONFIG_DEF;
  }

  @Override
  public Config validate(Map<String, String> connectorConfigs) {
    return super.validate(connectorConfigs);
  }

  @Override
  public String version() {
    final Properties props = new Properties();
    try {
      props.load(getClass().getResourceAsStream("/version.properties"));
    } catch (IOException ignore) {
    }
    return props.getProperty("version", "1.0");
  }
}

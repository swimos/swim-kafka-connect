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
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import swim.client.ClientRuntime;
import swim.kafka.connector.id.AgentUriParser;
import swim.structure.Value;
import swim.uri.Uri;

public class SwimKafkaSinkTask extends SinkTask {

  private static final SinkMessageConvertor MESSAGE_CONVERTOR = new SinkMessageConvertor();
  private static final AgentUriParser AGENT_URI_PARSER = new AgentUriParser();

  private ClientRuntime swimRef;
  private SwimSinkConfig config;

  @Override
  public void start(Map<String, String> props) {
    this.swimRef = new ClientRuntime();
    swimRef.start();
    this.config = new SwimSinkConfig(props);
  }

  @Override
  public void put(Collection<SinkRecord> records) {
    for(SinkRecord record: records) {
      final Value key = getKey(record);
      final Value value = getMessage(record);
      final Uri agentUri = parseAgentUri(key, value);
      if (!agentUri.isEmpty()) {
        swimRef.command(this.config.getHostUri(), agentUri, this.config.getLaneUri(), value);
      } else {
        throw new RuntimeException("Agent Uri is empty");
      }
    }
  }

  // To be used by sub-classes for overriding and to provide a specialized MessageConvertor
  protected SinkMessageConvertor getMessageConvertor() {
    return MESSAGE_CONVERTOR;
  }

  // To be used by sub-classes for overriding and to provide a specialized IdParser
  protected AgentUriParser getAgentUriParser() {
    return AGENT_URI_PARSER;
  }

  protected Value getKey(SinkRecord record) {
    return getMessageConvertor().convertKey(record);
  }

  protected Value getMessage(SinkRecord record) {
    return getMessageConvertor().convertValue(record);
  }

  protected Uri parseAgentUri(Value key, Value value) {
    return getAgentUriParser().computeAgentUri(key, value, this.config);
  }

  @Override
  public void stop() {

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

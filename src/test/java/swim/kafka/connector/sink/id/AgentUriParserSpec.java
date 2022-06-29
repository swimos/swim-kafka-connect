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

package swim.kafka.connector.sink.id;

import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;
import swim.kafka.connector.id.AgentUriParser;
import swim.kafka.connector.sink.SwimSinkConfig;
import swim.structure.Num;
import swim.structure.Record;
import swim.structure.Text;
import swim.structure.Value;
import swim.uri.Uri;
import static org.testng.Assert.assertEquals;
import static swim.kafka.connector.sink.SwimSinkConfig.SWIM_AGENT_ID_EXTRACTOR_PROP;
import static swim.kafka.connector.sink.SwimSinkConfig.SWIM_USE_VALUE_FIELD_FOR_AGENT_ID_PROP;
import static swim.kafka.connector.sink.SwimSinkConfig.SWIM_AGENT_URI_PATTERN_PROP;

public class AgentUriParserSpec {

  private final AgentUriParser agentUriParser = new AgentUriParser();

  @Test
  public void agentUriFromKey() {
    SwimSinkConfig swimSinkConfig = makeConfig("/user/:id", false);
    assertEquals(agentUriParser.computeAgentUri(Text.from("abcd"), Value.absent(), swimSinkConfig), Uri.parse("/user/abcd"));

    swimSinkConfig = makeConfig("/user/:id", false);
    assertEquals(agentUriParser.computeAgentUri(Num.from(101), Value.absent(), swimSinkConfig), Uri.parse("/user/101"));
  }

  @Test
  public void agentUriFromKeyAndGetSelector() {
    final SwimSinkConfig swimSinkConfig = makeConfig("/user/:id", false, "$key1");
    final Value key = Record.create(2).slot("key0", "value0").slot("key1", "abcd");
    assertEquals(agentUriParser.computeAgentUri(key, Value.absent(), swimSinkConfig), Uri.parse("/user/abcd"));
  }

  @Test
  public void agentUriFromKeyAndFieldSelector() {
    final SwimSinkConfig swimSinkConfig = makeConfig("/user/:id", false, "$foo.key1");
    final Value key = Record.create().slot("foo", Record.create(2).slot("key0", "value0").slot("key1", "abcd"));
    assertEquals(agentUriParser.computeAgentUri(key, Value.absent(), swimSinkConfig), Uri.parse("/user/abcd"));
  }

  @Test
  public void agentUriFromValue() {
    final SwimSinkConfig swimSinkConfig = makeConfig("/user/:id", true);
    assertEquals(agentUriParser.computeAgentUri(Value.absent(), Text.from("abcd"), swimSinkConfig), Uri.parse("/user/abcd"));
  }

  @Test
  public void agentUriFromValueAndGetSelector() {
    final SwimSinkConfig swimSinkConfig = makeConfig("/user/:id", true, "$key0");
    final Value value = Record.create(2).slot("key0", "abcd").slot("key1", "value1");
    assertEquals(agentUriParser.computeAgentUri(Value.absent(), value, swimSinkConfig), Uri.parse("/user/abcd"));
  }

  @Test
  public void invalidAgentUriFromKey() {
    final SwimSinkConfig swimSinkConfig = makeConfig("/user/:id", false, "key1");
    final Value key = Record.create(2).slot("key0", "value0").slot("key2", "abcd");
    assertEquals(agentUriParser.computeAgentUri(key, Value.absent(), swimSinkConfig), Uri.empty());
  }

  @Test
  public void invalidAgentUriFromValue() {
    final SwimSinkConfig swimSinkConfig = makeConfig("/user/:id", true, "key1");
    final Value value = Record.create(2).slot("key0", "value0").slot("key2", "abcd");
    assertEquals(agentUriParser.computeAgentUri(Value.absent(), value, swimSinkConfig), Uri.empty());
  }

  private SwimSinkConfig makeConfig(String agentUriPattern, boolean useValueForAgentId) {
    Map<String, String> props = new HashMap<>();
    props.put(SWIM_AGENT_URI_PATTERN_PROP, agentUriPattern);
    props.put(SWIM_USE_VALUE_FIELD_FOR_AGENT_ID_PROP, Boolean.toString(useValueForAgentId));
    return new SwimSinkConfig(props);
  }

  private SwimSinkConfig makeConfig(String agentUriPattern, boolean useValueForAgentId, String agentIdExtractor) {
    Map<String, String> props = new HashMap<>();
    props.put(SWIM_AGENT_URI_PATTERN_PROP, agentUriPattern);
    props.put(SWIM_USE_VALUE_FIELD_FOR_AGENT_ID_PROP, Boolean.toString(useValueForAgentId));
    props.put(SWIM_AGENT_ID_EXTRACTOR_PROP, agentIdExtractor);
    return new SwimSinkConfig(props);
  }

}

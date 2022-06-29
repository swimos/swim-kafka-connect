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

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.config.ConfigException;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import swim.structure.Selector;
import swim.structure.Value;
import swim.uri.Uri;
import swim.uri.UriPattern;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static swim.kafka.connector.sink.SwimSinkConfig.SWIM_AGENT_ID_EXTRACTOR_PROP;
import static swim.kafka.connector.sink.SwimSinkConfig.SWIM_AGENT_URI_PATTERN_PROP;
import static swim.kafka.connector.sink.SwimSinkConfig.SWIM_HOST_URI_PROP;
import static swim.kafka.connector.sink.SwimSinkConfig.SWIM_LANE_URI_PROP;
import static swim.kafka.connector.sink.SwimSinkConfig.SWIM_USE_VALUE_FIELD_FOR_AGENT_ID_PROP;

public class SwimSinkConfigSpec {

  private final Map<String, String> props = new HashMap<>();

  @BeforeTest
  public void clearProps() {
    this.props.clear();
  }

  @Test
  public void getHostUri() {
    validateHostUri("warp://localhost:9001", Uri.parse("warp://localhost:9001"));
    validateHostUri("warps://localhost:9001", Uri.parse("warps://localhost:9001"));
    validateHostUriError("localhost:9001");
    validateHostUriError("bad");
    validateHostUriError(null);
    validateHostUriError("");
  }

  private void validateHostUri(String hostUri, Uri expected) {
    final SwimSinkConfig config = makeConfig(SWIM_HOST_URI_PROP, hostUri);
    assertEquals(config.getHostUri(), expected);
  }

  private void validateHostUriError(String hostUri) {
    final SwimSinkConfig config = makeConfig(SWIM_HOST_URI_PROP, hostUri);
    assertThrows(ConfigException.class, () -> config.getHostUri());
  }

  @Test
  public void getAgentUriPattern() {
    validateAgentUriPattern("/myagent/:id", UriPattern.parse("/myagent/:id"));
    validateAgentUriPattern("/country/US/state/:id", UriPattern.parse("/country/US/state/:id"));
    validateAgentUriPatternError(null);
    validateAgentUriPatternError("");
  }

  private void validateAgentUriPattern(String agentUriPattern, UriPattern expected) {
    final SwimSinkConfig config = makeConfig(SWIM_AGENT_URI_PATTERN_PROP, agentUriPattern);
    assertEquals(config.getAgentUriPattern(), expected);
  }

  private void validateAgentUriPatternError(String agentUriPattern) {
    final SwimSinkConfig config = makeConfig(SWIM_AGENT_URI_PATTERN_PROP, agentUriPattern);
    assertThrows(ConfigException.class, () -> config.getAgentUriPattern());
  }

  @Test
  public void getLaneUri() {
    validateLaneUri("lane1", Uri.parse("lane1"));
    validateLaneUri("lane2", Uri.parse("lane2"));
    validateLaneUriError(null);
    validateLaneUriError("");
  }

  private void validateLaneUri(String laneUri, Uri expected) {
    final SwimSinkConfig config = makeConfig(SWIM_LANE_URI_PROP, laneUri);
    assertEquals(config.getLaneUri(), expected);
  }

  private void validateLaneUriError(String laneUri) {
    final SwimSinkConfig config = makeConfig(SWIM_LANE_URI_PROP, laneUri);
    assertThrows(ConfigException.class, () -> config.getLaneUri());
  }

  @Test
  public void useKeyOrValueFieldForAgentId() {
    validateKeyOrValueField("true", true);
    validateKeyOrValueField("false", false);
    validateKeyOrValueField("key", false);
    validateKeyOrValueField("value", false);
    validateKeyOrValueField(null, false);
    validateKeyOrValueField("", false);
  }

  private void validateKeyOrValueField(String agentIdField, boolean expected) {
    final SwimSinkConfig config = makeConfig(SWIM_USE_VALUE_FIELD_FOR_AGENT_ID_PROP, agentIdField);
    assertEquals(config.useValueForAgentId(), expected);
  }

  @Test
  public void getAgentIdExtractor() {
    validateAgentIdExtractor("$id", Selector.identity().get("id"));
    validateAgentIdExtractor("$id.key", Selector.identity().get("id").get("key"));
    validateAgentIdExtractor("", Selector.identity());
    validateAgentIdExtractorError("id");
    validateAgentIdExtractorError("id.key");
  }

  private void validateAgentIdExtractor(String agentIdExtractor, Value expected) {
    final SwimSinkConfig config = makeConfig(SWIM_AGENT_ID_EXTRACTOR_PROP, agentIdExtractor);
    assertEquals(config.getAgentIdExtractor(), expected);
  }

  private void validateAgentIdExtractorError(String agentIdExtractor) {
    final SwimSinkConfig config = makeConfig(SWIM_AGENT_ID_EXTRACTOR_PROP, agentIdExtractor);
    assertThrows(ConfigException.class, () -> config.getAgentIdExtractor());
  }

  private SwimSinkConfig makeConfig(String key, String value) {
    putProp(key, value);
    return new SwimSinkConfig(props);
  }

  private void putProp(String key, String value) {
    this.props.put(key, value);
  }

}

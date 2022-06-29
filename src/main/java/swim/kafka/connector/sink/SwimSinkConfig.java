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

import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import swim.codec.ParserException;
import swim.recon.Recon;
import swim.structure.Selector;
import swim.structure.Value;
import swim.uri.Uri;
import swim.uri.UriPattern;

public class SwimSinkConfig {

  public static final String SWIM_HOST_URI_PROP = "swim.host.uri";
  public static final String SWIM_AGENT_URI_PATTERN_PROP = "swim.agent.uri.pattern";
  public static final String SWIM_LANE_URI_PROP = "swim.lane.uri";
  public static final String SWIM_USE_VALUE_FIELD_FOR_AGENT_ID_PROP = "swim.use.value.field.for.agent.id";
  public static final String SWIM_AGENT_ID_EXTRACTOR_PROP = "swim.agent.id.extractor";


  public static ConfigDef.Validator SWIM_HOST_URI_VALIDATOR = hostUriValidator();
  public static ConfigDef.Validator SWIM_AGENT_URI_PATTERN_VALIDATOR = agentUriPatternValidator();
  public static ConfigDef.Validator SWIM_LANE_URI_VALIDATOR = laneUriValidator();
  public static ConfigDef.Validator SWIM_AGENT_ID_EXTRACTOR_VALIDATOR = agentIdExtractorValidator();

  public static ConfigDef SWIM_SINK_CONFIG_DEF = makeConfigDef();


  private final Map<String, String> props;

  public SwimSinkConfig(Map<String, String> props) {
    this.props = props;
  }

  public Uri getHostUri() throws ConfigException {
    final String value = getWithDefault(SWIM_HOST_URI_PROP);
    validateHostUri(SWIM_HOST_URI_PROP, value);
    return Uri.parse(value);
  }

  public UriPattern getAgentUriPattern() throws ConfigException {
    final String value = getWithDefault(SWIM_AGENT_URI_PATTERN_PROP);
    validateAgentUriPattern(SWIM_AGENT_URI_PATTERN_PROP, value);
    return UriPattern.parse(value);
  }

  public Uri getLaneUri() throws ConfigException {
    final String value = getWithDefault(SWIM_LANE_URI_PROP);
    validateUri(SWIM_LANE_URI_PROP, value);
    return Uri.parse(value);
  }

  public boolean useValueForAgentId() throws ConfigException {
    return getWithDefault(SWIM_USE_VALUE_FIELD_FOR_AGENT_ID_PROP, false);
  }

  public Selector getAgentIdExtractor() throws ConfigException {
    final String value = getWithDefault(SWIM_AGENT_ID_EXTRACTOR_PROP);
    validateAgentIdExtractor(SWIM_AGENT_ID_EXTRACTOR_PROP, value);
    if (value.equals("")) {
      return Selector.identity();
    } else {
      final Value selector = Recon.parse(value);
      if (selector instanceof Selector) {
        return (Selector) selector;
      } else {
        return Selector.identity();
      }
    }
  }

  private String getWithDefault(String propKey) {
    return getWithDefault(propKey, "");
  }

  private String getWithDefault(String propKey, String def) {
    final String value =  this.props.get(propKey);
    return value == null ? def : value;
  }

  private boolean getWithDefault(String propKey, boolean def) {
    final String value =  this.props.get(propKey);
    try {
      return Boolean.parseBoolean(value);
    } catch (Exception ignored) {
    }
    return def;
  }

  private static ConfigDef makeConfigDef() {
    final ConfigDef configDef = new ConfigDef();

    configDef.define(SWIM_HOST_URI_PROP, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
          SWIM_HOST_URI_VALIDATOR, ConfigDef.Importance.HIGH, "The Host Uri of the Swim Application");

    configDef.define(SWIM_AGENT_URI_PATTERN_PROP, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
          SWIM_AGENT_URI_PATTERN_VALIDATOR, ConfigDef.Importance.HIGH, "The Web Agent Uri Pattern");

    configDef.define(SWIM_LANE_URI_PROP, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
          SWIM_LANE_URI_VALIDATOR, ConfigDef.Importance.HIGH, "The Lane Uri of the Web Agent");

    configDef.define(SWIM_USE_VALUE_FIELD_FOR_AGENT_ID_PROP, ConfigDef.Type.BOOLEAN, false,
          ConfigDef.Importance.HIGH, "Whether to use the key field or value field of the Sink Record to extract the Id");

    configDef.define(SWIM_AGENT_ID_EXTRACTOR_PROP, ConfigDef.Type.STRING, "",
          SWIM_AGENT_ID_EXTRACTOR_VALIDATOR, ConfigDef.Importance.HIGH, "Recon selector expression to parse the id");

    return configDef;
  }

  private static ConfigDef.Validator hostUriValidator() {
    return (name, value) -> validateHostUri(name, value);
  }

  private static void validateHostUri(String name, Object object) {
    if (object == null) {
      throw new ConfigException(name, null, "Host uri not defined");
    }
    final String value = (String) object;
    if (value.startsWith("warp://") || value.startsWith("warps://")) {
      validateUri(name, value);
    } else {
      throw new ConfigException(name, value, "Must start with warp:// or warps://");
    }
  }

  private static ConfigDef.Validator agentUriPatternValidator() {
    return (name, value) -> validateAgentUriPattern(name, value);
  }

  private static void validateAgentUriPattern(String name, Object object) {
    if (object == null) {
      throw new ConfigException(name, null, "Agent Uri Pattern not defined");
    }
    final String value = (String) object;
    UriPattern uriPattern = UriPattern.empty();
    try {
      uriPattern = UriPattern.parse(value);
    } catch (Exception ignored) {
    }
    if (uriPattern.toUri().isEmpty()) {
      throw new ConfigException(name, value, "Not a valid Agent Uri Pattern");
    }
  }

  private static ConfigDef.Validator laneUriValidator() {
    return (name, value) -> validateUri(name, value);
  }

  private static void validateUri(String name, Object object) {
    if (object == null) {
      throw new ConfigException(name, null, "Lane Uri not defined");
    }
    final String value = (String) object;
    Uri uri = Uri.empty();
    try {
      uri = Uri.parse(value);
    } catch (Exception ignored) {
    }
    if (uri.isEmpty()) {
      throw new ConfigException(name, value, "Not a valid Uri");
    }
  }

  private static ConfigDef.Validator agentIdExtractorValidator() {
    return (name, value) -> validateAgentIdExtractor(name, (String) value);
  }

  private static void validateAgentIdExtractor(String name, String value) {
    if (value == null || value.equals("")) {
      return;
    }
    Value selector = null;
    try {
      selector = Recon.parse(value);
    } catch (ParserException ignored) {
    }
    if (!(selector instanceof Selector)) {
      throw new ConfigException(name, value, "Not a valid Recon Selector Expression");
    }
  }

}

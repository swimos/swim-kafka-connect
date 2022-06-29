// Copyright 2015-present SWIM Inc.
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

package swim.kafka.connector.id;

import swim.kafka.connector.sink.SwimSinkConfig;
import swim.structure.Item;
import swim.structure.Selector;
import swim.structure.Value;
import swim.uri.Uri;
import swim.uri.UriPattern;

public class AgentUriParser {

  public AgentUriParser() {

  }

  public Uri computeAgentUri(Value key, Value value, SwimSinkConfig config) {
    try {
      final boolean useValueForAgentId = config.useValueForAgentId();
      final Selector agentIdExtractor = config.getAgentIdExtractor();
      final Item id;
      if (useValueForAgentId) {
        id = agentIdExtractor.evaluate(value);
        
      } else {
        id = agentIdExtractor.evaluate(key);
      }
      if (id.isDefined()) {
        final String idStr = id.stringValue("");
        if (!idStr.equals("")) {
          final UriPattern agentUriPattern = config.getAgentUriPattern();
          return agentUriPattern.apply(idStr);
        }
      }
    } catch (Exception e) {
    }
    return Uri.empty();
  }

}

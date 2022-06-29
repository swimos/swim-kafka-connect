// Copyright 2015-2022 SWIM.AI inc.
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

package swim.app;

import swim.api.SwimLane;
import swim.api.agent.AbstractAgent;
import swim.api.http.HttpLane;
import swim.api.lane.ValueLane;
import swim.codec.Output;
import swim.http.HttpChunked;
import swim.http.HttpEntity;
import swim.http.HttpResponse;
import swim.http.HttpStatus;
import swim.http.MediaType;
import swim.recon.Recon;
import swim.structure.Value;

public class LatestValue extends AbstractAgent {

  @SwimLane("latest")
  protected ValueLane<Value> latest = valueLane();

  @SwimLane("latestData")
  protected HttpLane<Value> latestData = this.<Value>httpLane().doRespond(request -> {
    final Value payload = this.latest.get();
    // Construct the response entity by incrementally serializing and encoding
    final HttpEntity<?> entity = HttpChunked.from(Recon.write(payload, Output.full()),
          MediaType.applicationXRecon());
    // Return the HTTP response.
    return HttpResponse.from(HttpStatus.OK).content(entity);
  });

  @Override
  public void didStart() {
    info(nodeUri() + ": didStart");
  }

}

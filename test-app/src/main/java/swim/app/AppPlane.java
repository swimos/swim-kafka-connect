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

import swim.api.plane.AbstractPlane;
import swim.api.plane.PlaneContext;
import swim.kernel.Kernel;
import swim.server.ServerLoader;

public class AppPlane extends AbstractPlane {

  public static void main(String[] args) {
    // Load the SwimOS kernel, loading its configuration from the
    // `server.recon` Java resource.
    final Kernel kernel = ServerLoader.loadServer();
    // Get a handle to the configured application plane.
    final PlaneContext plane = (PlaneContext) kernel.getSpace("swim-plane");

    // Boot the SwimOS kernel.
    kernel.start();
    System.out.println("Running Swim Plane ...");

    // Park the main thread while the application concurrently runs.
    kernel.run();
  }

}

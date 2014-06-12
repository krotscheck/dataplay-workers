/*
 * Copyright (c) 2014 Michael Krotscheck
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy
 * of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.dataplay.storm;

/**
 * A collection of commands that may be issued to the bolt management stream.
 *
 * @author Michael Krotscheck
 */
public final class TopologyCommand {

    /**
     * Private constructor.
     */
    private TopologyCommand() {
    }

    /**
     * This command tells the bolt to clean up and shut down. It is issued as a
     * courtesy to the bolt, so that it can close any outgoing connections it
     * has to other systems. Since regular topologies tend to assume continuous
     * operation, this is only necessary in situations where the tuple stream is
     * finite.
     */
    public static final String SHUTDOWN = "shutdown";
}

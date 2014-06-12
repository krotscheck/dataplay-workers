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

package io.dataplay.util;

import io.dataplay.storm.Stream;
import io.dataplay.storm.TopologyCommand;
import com.google.common.base.Strings;

import backtype.storm.Constants;
import backtype.storm.tuple.Tuple;

/**
 * A collection of static utility methods useful when manipulating storm
 * topologies, spouts, and bolts.
 *
 * @author Michael Krotscheck
 */
public final class StormUtil {

    /**
     * Private constructor.
     */
    private StormUtil() {

    }

    /**
     * Tells us whether this is a tick tuple or not.
     *
     * @param tuple The tuple to check.
     * @return True if it's a tick tuple, otherwise false.
     */
    public static boolean isTickTuple(final Tuple tuple) {
        return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
                && tuple.getSourceStreamId().equals(
                Constants.SYSTEM_TICK_STREAM_ID);
    }

    /**
     * Tells us whether this is a shutdown tuple or not.
     *
     * @param tuple The tuple to check.
     * @return True if it's a shutdown tuple, otherwise false.
     */
    public static boolean isShutdownTuple(final Tuple tuple) {
        if (tuple.getSourceStreamId()
                .equals(Stream.BOLT_MANAGEMENT.getName())) {
            String command = tuple.getString(Stream.BOLT_MANAGEMENT.getFields()
                    .fieldIndex("command"));
            return !Strings.isNullOrEmpty(command)
                    && command.equals(TopologyCommand.SHUTDOWN);
        }
        return false;
    }
}

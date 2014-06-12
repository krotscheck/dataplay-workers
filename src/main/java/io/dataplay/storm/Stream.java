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

import backtype.storm.tuple.Fields;

/**
 * A collection of stream identifiers used for our topology coordination plane.
 * These are used by our coordination bolt to ensure that topologies are only
 * shut down when the last tuple has been handled.
 *
 * @author Michael Krotscheck
 */
public final class Stream {

    /**
     * Get the name of this stream.
     *
     * @return The stream's name
     */
    public String getName() {
        return name;
    }

    /**
     * Get the fields for this stream.
     *
     * @return The stream's fields
     */
    public Fields getFields() {
        return fields;
    }

    /**
     * The name of this stream.
     */
    private final String name;

    /**
     * The fields emitted within this stream.
     */
    private final Fields fields;

    /**
     * Creates a new stream descriptor.
     *
     * @param streamName   The name of the stream.
     * @param streamFields The fields which tuples in this stream will send.
     */
    private Stream(final String streamName, final Fields streamFields) {
        this.name = streamName;
        this.fields = streamFields;
    }

    /**
     * Our bolt management stream.
     */
    public static final Stream BOLT_MANAGEMENT = new Stream("bolt_manager",
            new Fields("command"));

    /**
     * Our generic management stream.
     */
    public static final Stream STATUS = new Stream("worker_status",
            new Fields("componentId", "threadId", "state"));

}

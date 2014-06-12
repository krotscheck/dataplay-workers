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

package io.dataplay.test;

import io.dataplay.storm.Stream;
import org.apache.commons.lang.RandomStringUtils;

import java.util.ArrayList;
import java.util.List;

import backtype.storm.Constants;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * This utility assists in creating tuples for testing.
 *
 * @author Michael Krotscheck
 */
public final class TupleUtil {

    /**
     * Private constructor.
     */
    private TupleUtil() {
    }

    /**
     * Creates a mock tick tuple.
     *
     * @return A new tick tuple.
     */
    public static Tuple mockTickTuple() {
        return mockTuple(Constants.SYSTEM_COMPONENT_ID,
                Constants.SYSTEM_TICK_STREAM_ID);
    }

    /**
     * Create a mock tuple for a given component and stream.
     *
     * @param componentId The Component ID.
     * @param streamId    The Stream ID
     * @return A new tuple.
     */
    public static Tuple mockTuple(final String componentId,
                                  final String streamId) {
        return mockTuple(componentId, streamId, new Fields(),
                new ArrayList<>());
    }

    /**
     * Create a mock tuple for a given component, stream, with data and fields.
     *
     * @param componentId The Component ID.
     * @param streamId    The Stream ID
     * @param fields      Fields.
     * @param data        The data to include in the tuple.
     * @return A new tuple.
     */
    public static Tuple mockTuple(final String componentId,
                                  final String streamId,
                                  final Fields fields,
                                  final List<Object> data) {
        Tuple tuple = mock(Tuple.class);
        when(tuple.getSourceComponent()).thenReturn(componentId);
        when(tuple.getSourceStreamId()).thenReturn(streamId);
        when(tuple.getFields()).thenReturn(fields);
        when(tuple.getValues()).thenReturn(data);

        int fieldCount = fields.size();
        for (int i = 0; i < fieldCount; i++) {
            when(tuple.getString(i)).thenReturn(data.get(i).toString());
        }
        return tuple;
    }

    /**
     * Generate a generic tuple with random data.
     *
     * @return A mock data tuple with random data.
     */
    public static Tuple mockDataTuple() {

        List<String> fieldData = new ArrayList<>();
        List<Object> valueData = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            fieldData.add(RandomStringUtils.randomAlphanumeric(5));
            valueData.add(RandomStringUtils.randomAlphanumeric(10));
        }

        return mockTuple(Constants.SYSTEM_EXECUTOR_ID.toString(),
                Utils.DEFAULT_STREAM_ID,
                new Fields(fieldData),
                valueData);
    }

    /**
     * Create a mock command tuple.
     *
     * @param command The command to issue.
     * @return A new tuple.
     */
    public static Tuple mockCommandTuple(final String command) {

        List<Object> data = new ArrayList<>();
        data.add(command);

        return mockTuple(Constants.SYSTEM_EXECUTOR_ID.toString(),
                Stream.BOLT_MANAGEMENT.getName(),
                Stream.BOLT_MANAGEMENT.getFields(),
                data);
    }
}

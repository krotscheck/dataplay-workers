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

package io.dataplay.storm.workers.noop;

import io.dataplay.test.TupleUtil;
import io.dataplay.test.UnitTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the Noop Bolt.
 *
 * @author Michael Krotscheck
 */
@Category(UnitTest.class)
@RunWith(PowerMockRunner.class)
@PrepareForTest(LoggerFactory.class)
public final class NoopBoltTest {

    /**
     * Assert that the schema calculation acts as a passthrough.
     */
    @Test
    public void testCalculateSchema() {
        NoopBolt bolt = new NoopBolt();

        List<String> schema1 = new ArrayList<>();
        schema1.add("one");
        schema1.add("two");
        Fields fields1 = new Fields(schema1);

        List<String> schema2 = new ArrayList<>();
        schema2.add("three");
        schema2.add("four");
        Fields fields2 = new Fields(schema2);

        List<Fields> fieldsList = new ArrayList<>();
        fieldsList.add(fields1);
        fieldsList.add(fields2);

        bolt.calculateFields(fieldsList);
        Fields result = bolt.getFields();

        Assert.assertEquals("one", result.get(0));
        Assert.assertEquals("two", result.get(1));
        Assert.assertEquals("three", result.get(2));
        Assert.assertEquals("four", result.get(3));
    }

    /**
     * Assert that calling process does nothing.
     */
    @Test
    public void testProcess() {
        // Test logger.
        Logger mockLogger = mock(Logger.class);
        PowerMockito.mockStatic(LoggerFactory.class);
        when(LoggerFactory.getLogger(any(Class.class))).thenReturn(mockLogger);

        // Test data
        Tuple t = TupleUtil.mockDataTuple();
        NoopBolt bolt = new NoopBolt();

        // Test mocks.
        Map<String, Object> config = new HashMap<>();
        TopologyContext context = mock(TopologyContext.class);
        OutputCollector outputCollector = mock(OutputCollector.class);

        // Set up the bolt.
        bolt.prepare(config, context, outputCollector);

        // Execute...
        bolt.execute(t);

        verify(mockLogger).info(t.toString());
        verify(outputCollector).ack(t);
    }

    /**
     * Test that the tick is logged.
     */
    @Test
    public void testTick() {
        // Test logger.
        Logger mockLogger = mock(Logger.class);
        PowerMockito.mockStatic(LoggerFactory.class);
        when(LoggerFactory.getLogger(any(Class.class))).thenReturn(mockLogger);

        // Test mocks.
        NoopBolt bolt = new NoopBolt();

        // Set up the bolt.
        bolt.tick();

        verify(mockLogger).info("Tick");
    }

    /**
     * Assert that the isValid method only returns true if the logger is valid.
     */
    @Test
    public void testIsValid() {

        // Test mocks.
        NoopBolt bolt = new NoopBolt();

        // Default must be valid.
        Assert.assertTrue(bolt.isValid());
    }
}

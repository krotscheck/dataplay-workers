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

package io.dataplay.storm.workers.logging;

import io.dataplay.test.UnitTest;
import io.dataplay.test.TupleUtil;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Unit tests for our logging bolt.
 */
@Category(UnitTest.class)
public final class LoggingBoltTest {

    /**
     * Assert that the schema calculation acts as a passthrough.
     */
    @Test
    public void testCalculateSchema() {
        LoggingBolt bolt = new LoggingBolt();

        Map<String, String> schema1 = new HashMap<>();
        schema1.put("one", "string");
        schema1.put("two", "string");

        Map<String, String> schema2 = new HashMap<>();
        schema2.put("three", "string");
        schema2.put("four", "string");

        List<Map<String, String>> schemaList = new ArrayList<>();
        schemaList.add(schema1);
        schemaList.add(schema2);

        Map<String, String> calculatedSchema = bolt.calculateSchema(schemaList);

        Assert.assertTrue(calculatedSchema.entrySet()
                .containsAll(schema1.entrySet()));
        Assert.assertTrue(calculatedSchema.entrySet()
                .containsAll(schema2.entrySet()));
    }

    /**
     * Assert that we can override the logger.
     */
    @Test
    public void getGetSetLogger() {

        Logger mockLogger = mock(Logger.class);
        LoggingBolt bolt = new LoggingBolt();

        Assert.assertNotNull(bolt.getLogger());
        bolt.setLogger(mockLogger);
        Assert.assertEquals(mockLogger, bolt.getLogger());
    }

    /**
     * Assert that calling process does nothing.
     */
    @Test
    public void testProcess() {

        // Test data
        Tuple t = TupleUtil.mockDataTuple();
        LoggingBolt bolt = new LoggingBolt();

        // Test mocks.
        Logger mockLogger = mock(Logger.class);
        Map<String, Object> config = new HashMap<>();
        TopologyContext context = mock(TopologyContext.class);
        OutputCollector outputCollector = mock(OutputCollector.class);

        // Set up the bolt.
        bolt.prepare(config, context, outputCollector);
        bolt.setLogger(mockLogger);

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

        // Test mocks.
        LoggingBolt bolt = new LoggingBolt();
        Logger mockLogger = mock(Logger.class);

        // Set up the bolt.
        bolt.setLogger(mockLogger);
        bolt.tick();

        verify(mockLogger).info("Tick");
    }

    /**
     * Assert that the isValid method only returns true if the logger is valid.
     */
    @Test
    public void testIsValid() {

        // Test mocks.
        LoggingBolt bolt = new LoggingBolt();
        Logger mockLogger = mock(Logger.class);

        // Default must be valid.
        Assert.assertTrue(bolt.isValid());

        // Try setting null
        bolt.setLogger(null);

        // Default must be valid.
        Assert.assertFalse(bolt.isValid());

        // Change it up.
        bolt.setLogger(mockLogger);

        // We're valid again!
        Assert.assertTrue(bolt.isValid());
    }
}

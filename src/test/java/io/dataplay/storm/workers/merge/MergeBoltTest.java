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

package io.dataplay.storm.workers.merge;

import io.dataplay.test.TupleUtil;
import io.dataplay.test.UnitTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
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
import backtype.storm.utils.Utils;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for our merge bolt.
 */
@Category(UnitTest.class)
@RunWith(PowerMockRunner.class)
@PrepareForTest(LoggerFactory.class)
public final class MergeBoltTest {

    /**
     * Test that the tick method is properly invoked.
     */
    @Test
    public void testTick() {
        Logger mockLogger = mock(Logger.class);
        PowerMockito.mockStatic(LoggerFactory.class);
        when(LoggerFactory.getLogger(any(Class.class))).thenReturn(mockLogger);

        // Test mocks.
        MergeBolt bolt = new MergeBolt();
        bolt.tick();

        verify(mockLogger).debug("Tick");
    }

    /**
     * Assert that a merged fields is properly generated for this bolt.
     */
    @Test
    public void testCalculateBasicFields() {
        MergeBolt bolt = new MergeBolt();

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
     * Assert that a merged fields is properly generated for this bolt.
     */
    @Test
    public void testCalculateOverlapFields() {
        MergeBolt bolt = new MergeBolt();

        List<String> schema1 = new ArrayList<>();
        schema1.add("two");
        Fields fields1 = new Fields(schema1);

        List<String> schema2 = new ArrayList<>();
        schema2.add("two");
        schema2.add("four");
        Fields fields2 = new Fields(schema2);

        List<Fields> fieldsList = new ArrayList<>();
        fieldsList.add(fields1);
        fieldsList.add(fields2);

        bolt.calculateFields(fieldsList);
        Fields result = bolt.getFields();

        Assert.assertEquals("two", result.get(0));
        Assert.assertEquals("four", result.get(1));
    }

    /**
     * Assert that processing bolts merges tuples as expected.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testProcess() {
        MergeBolt bolt = new MergeBolt();
        Map<String, Object> config = new HashMap<>();
        TopologyContext context = mock(TopologyContext.class);
        OutputCollector outputCollector = mock(OutputCollector.class);

        ArgumentCaptor<List> emitCaptor = ArgumentCaptor.forClass(List.class);

        List<String> schema = new ArrayList<>();
        schema.add("one");
        schema.add("two");
        bolt.setFields(new Fields(schema));
        bolt.prepare(config, context, outputCollector);

        // ========================================
        Tuple one = TupleUtil.mockDataTuple(
                new String[]{"one", "two"},
                new String[]{"one", "two"}
        );
        bolt.process(one);
        verify(outputCollector).emit(
                eq(Utils.DEFAULT_STREAM_ID),
                eq(one),
                emitCaptor.capture()
        );
        List<Object> emitOne = emitCaptor.getValue();

        Assert.assertTrue(emitOne.get(0).equals("one"));
        Assert.assertTrue(emitOne.get(1).equals("two"));

        // ========================================
        Tuple two = TupleUtil.mockDataTuple(
                new String[]{"one"},
                new String[]{"one"}
        );
        bolt.process(two);
        verify(outputCollector).emit(
                eq(Utils.DEFAULT_STREAM_ID),
                eq(two),
                emitCaptor.capture()
        );
        List<Object> emitTwo = emitCaptor.getValue();

        Assert.assertTrue(emitTwo.get(0).equals("one"));
        Assert.assertTrue(emitTwo.get(1).equals(""));

        // ========================================
        Tuple three = TupleUtil.mockDataTuple(
                new String[]{"two"},
                new String[]{"two"}
        );
        bolt.process(three);
        verify(outputCollector).emit(
                eq(Utils.DEFAULT_STREAM_ID),
                eq(three),
                emitCaptor.capture()
        );
        List<Object> emitThree = emitCaptor.getValue();

        Assert.assertTrue(emitThree.get(0).equals(""));
        Assert.assertTrue(emitThree.get(1).equals("two"));
    }

    /**
     * Assert that a bolt without a fields is invalid.
     */
    @Test
    public void testIsValidWithoutFields() {
        MergeBolt bolt = new MergeBolt();
        Assert.assertNull(bolt.getFields());
        Assert.assertFalse(bolt.isValid());
    }

    /**
     * Assert that the bolt needs a fields to be valid.
     */
    @Test
    public void testIsValid() {
        MergeBolt bolt = new MergeBolt();

        bolt.setFields(new Fields());

        Assert.assertTrue(bolt.isValid());
    }
}

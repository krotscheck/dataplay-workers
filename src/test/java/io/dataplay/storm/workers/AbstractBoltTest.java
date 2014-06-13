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

package io.dataplay.storm.workers;

import io.dataplay.storm.Stream;
import io.dataplay.storm.TopologyCommand;
import io.dataplay.test.TupleUtil;
import io.dataplay.test.UnitTest;
import org.apache.commons.lang.ArrayUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;

import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Unit test for the abstract bolt.
 *
 * @author Michael Krotscheck
 */
@Category(UnitTest.class)
public final class AbstractBoltTest {

    /**
     * Tear down our test.
     */
    @After
    public void teardown() {
        Mockito.reset();
    }

    /**
     * Assert that the bolt configures itself appropriately.
     */
    @Test
    public void testPrepare() {
        AbstractBolt bolt = mock(AbstractBolt.class,
                Mockito.CALLS_REAL_METHODS);

        Map<String, Object> config = new HashMap<>();
        TopologyContext context = mock(TopologyContext.class);
        OutputCollector outputCollector = mock(OutputCollector.class);

        bolt.prepare(config, context, outputCollector);

        Assert.assertEquals(config, bolt.getBoltConfig());
        Assert.assertEquals(context, bolt.getContext());
        Assert.assertEquals(outputCollector, bolt.getBoltOutputCollector());
    }

    /**
     * Test the fields getter/setter.
     */
    @Test
    public void testGetSetFields() {
        AbstractBolt bolt = mock(AbstractBolt.class,
                Mockito.CALLS_REAL_METHODS);

        Fields fields = new Fields();

        Assert.assertNull(bolt.getFields());

        bolt.setFields(fields);

        Assert.assertEquals(fields, bolt.getFields());
    }

    /**
     * Test the declaration of output fields.
     */
    @Test
    public void testDeclareOutputFields() {
        AbstractBolt bolt = mock(AbstractBolt.class,
                Mockito.CALLS_REAL_METHODS);
        OutputFieldsDeclarer declarer = mock(OutputFieldsDeclarer.class);

        List<String> fieldsList = new ArrayList<>();
        fieldsList.add("one");
        fieldsList.add("two");
        Fields fields = new Fields(fieldsList);

        bolt.setFields(fields);
        bolt.declareOutputFields(declarer);

        // Assert that the correct fields were declared.
        verify(declarer).declareStream(
                eq(Utils.DEFAULT_STREAM_ID),
                eq(fields)
        );

        // Assert that the correct streams were declared.
        verify(declarer).declareStream(
                eq(Stream.STATUS.getName()),
                eq(Stream.STATUS.getFields())
        );
    }

    /**
     * Test emitting one tuple with one anchor to the default stream.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testEmitTupleAnchor() {
        AbstractBolt bolt = mock(AbstractBolt.class,
                Mockito.CALLS_REAL_METHODS);
        Map<String, Object> config = new HashMap<>();
        TopologyContext context = mock(TopologyContext.class);
        OutputCollector outputCollector = mock(OutputCollector.class);
        Tuple anchor = mock(Tuple.class);
        List data = mock(List.class);

        // Prepare the bolt
        bolt.prepare(config, context, outputCollector);
        bolt.emit(anchor, data);

        verify(outputCollector).emit(
                eq(Utils.DEFAULT_STREAM_ID), eq(anchor), eq(data)
        );
    }

    /**
     * Test emitting one tuple with multiple anchors to the default stream.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testEmitTupleAnchors() {
        AbstractBolt bolt = mock(AbstractBolt.class,
                Mockito.CALLS_REAL_METHODS);
        Map<String, Object> config = new HashMap<>();
        TopologyContext context = mock(TopologyContext.class);
        OutputCollector outputCollector = mock(OutputCollector.class);
        List<Tuple> anchors = new ArrayList<>();
        anchors.add(mock(Tuple.class));
        anchors.add(mock(Tuple.class));
        List data = mock(List.class);

        // Prepare the bolt
        bolt.prepare(config, context, outputCollector);
        bolt.emit(anchors, data);

        verify(outputCollector).emit(
                eq(Utils.DEFAULT_STREAM_ID), eq(anchors), eq(data)
        );
    }

    /**
     * Test emitting one tuple with one anchor to a specified stream.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testEmitStreamTupleAnchor() {
        AbstractBolt bolt = mock(AbstractBolt.class,
                Mockito.CALLS_REAL_METHODS);
        Map<String, Object> config = new HashMap<>();
        TopologyContext context = mock(TopologyContext.class);
        OutputCollector outputCollector = mock(OutputCollector.class);
        Tuple anchor = mock(Tuple.class);
        List data = mock(List.class);

        // Prepare the bolt
        bolt.prepare(config, context, outputCollector);
        bolt.emit(Stream.BOLT_MANAGEMENT.getName(), anchor, data);

        verify(outputCollector).emit(
                eq(Stream.BOLT_MANAGEMENT.getName()),
                eq(anchor),
                eq(data)
        );
    }

    /**
     * Test emitting one tuple with multiple anchors to a specified stream.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testEmitStreamTupleAnchors() {
        AbstractBolt bolt = mock(AbstractBolt.class,
                Mockito.CALLS_REAL_METHODS);
        Map<String, Object> config = new HashMap<>();
        TopologyContext context = mock(TopologyContext.class);
        OutputCollector outputCollector = mock(OutputCollector.class);
        List<Tuple> anchors = new ArrayList<>();
        anchors.add(mock(Tuple.class));
        anchors.add(mock(Tuple.class));
        List data = mock(List.class);

        // Prepare the bolt
        bolt.prepare(config, context, outputCollector);
        bolt.emit(Stream.BOLT_MANAGEMENT.getName(), anchors, data);

        verify(outputCollector).emit(
                eq(Stream.BOLT_MANAGEMENT.getName()),
                eq(anchors),
                eq(data)
        );
    }

    /**
     * Test ack.
     */
    @Test
    public void testAck() {
        AbstractBolt bolt = mock(AbstractBolt.class,
                Mockito.CALLS_REAL_METHODS);
        Map<String, Object> config = new HashMap<>();
        TopologyContext context = mock(TopologyContext.class);
        OutputCollector outputCollector = mock(OutputCollector.class);

        Tuple tuple = mock(Tuple.class);

        // Prepare the bolt
        bolt.prepare(config, context, outputCollector);
        bolt.ack(tuple);

        verify(outputCollector).ack(eq(tuple));
    }

    /**
     * Test fail.
     */
    @Test
    public void testFail() {
        AbstractBolt bolt = mock(AbstractBolt.class,
                Mockito.CALLS_REAL_METHODS);
        Map<String, Object> config = new HashMap<>();
        TopologyContext context = mock(TopologyContext.class);
        OutputCollector outputCollector = mock(OutputCollector.class);

        Tuple tuple = mock(Tuple.class);

        // Prepare the bolt
        bolt.prepare(config, context, outputCollector);
        bolt.fail(tuple);

        verify(outputCollector).fail(eq(tuple));
    }

    /**
     * Test report error.
     */
    @Test
    public void testReportError() {
        AbstractBolt bolt = mock(AbstractBolt.class,
                Mockito.CALLS_REAL_METHODS);
        Map<String, Object> config = new HashMap<>();
        TopologyContext context = mock(TopologyContext.class);
        OutputCollector outputCollector = mock(OutputCollector.class);

        Throwable t = mock(Throwable.class);

        // Prepare the bolt
        bolt.prepare(config, context, outputCollector);
        bolt.reportError(t);

        verify(outputCollector).reportError(eq(t));
    }

    /**
     * Test basic calculate fields method.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testCalculateFields() {
        AbstractBolt bolt = mock(AbstractBolt.class);

        // Get our capture mechanism ready
        ArgumentCaptor<List> captor = ArgumentCaptor.forClass(List.class);

        // Prepare the bolt
        bolt.calculateFields();

        // Make sure the capture succeeded.
        verify(bolt).calculateFields(captor.capture());

        // Make sure the capture is empty.
        List result = captor.getValue();
        Assert.assertEquals(0, result.size());
    }

    /**
     * Pass a single fields into the bolt.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testCalculateFields1() {
        AbstractBolt bolt = mock(AbstractBolt.class);

        // Get our capture mechanism ready
        ArgumentCaptor<List> captor = ArgumentCaptor.forClass(List.class);

        List<String> schema = new ArrayList<>();
        schema.add("one");
        schema.add("two");
        Fields fields = new Fields(schema);

        // Prepare the bolt
        bolt.calculateFields(fields);

        // Make sure the capture succeeded.
        verify(bolt).calculateFields(captor.capture());

        // Make sure the capture is empty.
        List result = captor.getValue();
        Assert.assertEquals(fields, result.get(0));
    }

    /**
     * Pass a list of fields into the bolt.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testCalculateFields2() {
        AbstractBolt bolt = mock(AbstractBolt.class);

        List<String> schema = new ArrayList<>();
        schema.add("one");
        schema.add("two");
        Fields fields = new Fields(schema);

        List<Fields> fieldsList = new ArrayList<>();
        fieldsList.add(fields);

        // Prepare the bolt
        bolt.calculateFields(fieldsList);

        // Make sure the capture succeeded.
        verify(bolt).calculateFields(fieldsList);
    }

    /**
     * Assert that the merge fields works.
     */
    @Test
    public void testMergeFields() {
        AbstractBolt bolt = mock(AbstractBolt.class,
                Mockito.CALLS_REAL_METHODS);


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

        Fields result = bolt.mergeFields(fieldsList);

        Assert.assertTrue(result.get(0).equals("one"));
        Assert.assertTrue(result.get(1).equals("two"));
        Assert.assertTrue(result.get(2).equals("three"));
        Assert.assertTrue(result.get(3).equals("four"));
    }

    /**
     * Assert that the overlap fields works.
     */
    @Test
    public void testMergeOverlapFields() {
        AbstractBolt bolt = mock(AbstractBolt.class,
                Mockito.CALLS_REAL_METHODS);

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

        Fields result = bolt.mergeFields(fieldsList);

        Assert.assertTrue(result.get(0).equals("two"));
        Assert.assertTrue(result.get(1).equals("four"));
    }

    /**
     * Make sure that the shutdown command is executed.
     */
    @Test
    public void testExecuteShutdown() {
        AbstractBolt bolt = mock(AbstractBolt.class);
        Map<String, Object> config = new HashMap<>();
        TopologyContext context = mock(TopologyContext.class);
        OutputCollector outputCollector = mock(OutputCollector.class);
        bolt.prepare(config, context, outputCollector);

        Tuple tuple = TupleUtil.mockCommandTuple(TopologyCommand.SHUTDOWN);

        bolt.execute(tuple);

        verify(bolt).cleanup();
        verify(outputCollector).ack(eq(tuple));
    }

    /**
     * Make sure that the tick command is executed.
     */
    @Test
    public void testExecuteTick() {
        AbstractBolt bolt = mock(AbstractBolt.class);
        Map<String, Object> config = new HashMap<>();
        TopologyContext context = mock(TopologyContext.class);
        OutputCollector outputCollector = mock(OutputCollector.class);
        bolt.prepare(config, context, outputCollector);

        Tuple tuple = TupleUtil.mockTickTuple();

        bolt.execute(tuple);

        verify(bolt).tick();
        verify(outputCollector).ack(eq(tuple));
    }

    /**
     * Make sure that the process command is executed.
     */
    @Test
    public void testExecute() {
        AbstractBolt bolt = mock(AbstractBolt.class);
        Map<String, Object> config = new HashMap<>();
        TopologyContext context = mock(TopologyContext.class);
        OutputCollector outputCollector = mock(OutputCollector.class);
        bolt.prepare(config, context, outputCollector);

        Tuple tuple = TupleUtil.mockDataTuple();

        bolt.execute(tuple);

        verify(bolt).process(eq(tuple));
        verify(outputCollector).ack(eq(tuple));
    }

    /**
     * Ensure the constructor is abstract.
     *
     * @throws Exception Creating an abstract class should throw an error.
     */
    @Test
    public void testConstructor() throws Exception {
        Constructor<AbstractBolt> constructor = AbstractBolt.class
                .getDeclaredConstructor();

        // Must not be accessible (abstract)
        Assert.assertFalse(constructor.isAccessible());

        // Assert interface
        Class[] interfaces = AbstractBolt.class.getInterfaces();
        Assert.assertTrue(ArrayUtils.contains(interfaces, IDataWorker.class));


        // Override the private constructor and create an instance
        try {
            constructor.newInstance();
            Assert.fail("Constructor must not be accessible");
        } catch (InstantiationException e) {
            Assert.assertTrue(true);
        }

        // Assert extensibility.
        IDataWorker worker = new WorkerImpl();
        Assert.assertNotNull(worker);

    }

    /**
     * Private implementation for testing purposes.
     */
    private class WorkerImpl extends AbstractBolt {

        /**
         * Are we valid?
         *
         * @return True if valid, otherwise false.
         */
        @Override
        public Boolean isValid() {
            return true;
        }

        /**
         * Calculate the fields.
         *
         * @param parentFields The parent fields from which to calculate this
         *                     one.
         */
        @Override
        public void calculateFields(final List<Fields> parentFields) {
            setFields(mergeFields(parentFields));
        }

        /**
         * Ack the tuple.
         *
         * @param tuple Tuple to process.
         */
        @Override
        protected void process(final Tuple tuple) {
            getBoltOutputCollector().ack(tuple);
        }

        @Override
        protected void tick() {

        }
    }
}

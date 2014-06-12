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

import io.dataplay.UnitTest;
import io.dataplay.storm.TopologyCommand;
import io.dataplay.test.TupleUtil;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;

import backtype.storm.Constants;
import backtype.storm.tuple.Tuple;

/**
 * Unit test for the storm utility.
 *
 * @author Michael Krotscheck
 */
@Category(UnitTest.class)
public class StormUtilTest {

    /**
     * Test the tick tuple method.
     */
    @Test
    public final void testIsTickTuple() {

        // Create a tick tuple
        Tuple tickTuple = TupleUtil.mockTickTuple();
        Assert.assertTrue(StormUtil.isTickTuple(tickTuple));

        // Create a bogus tick tuple
        Tuple bogusTickTuple = TupleUtil.mockTuple(
                Constants.SYSTEM_EXECUTOR_ID.toString(),
                Constants.SYSTEM_TICK_STREAM_ID);
        Assert.assertFalse(StormUtil.isTickTuple(bogusTickTuple));

        // Create a bogus stream tuple
        Tuple bogusStreamTuple = TupleUtil.mockTuple(
                Constants.SYSTEM_COMPONENT_ID,
                Constants.COORDINATED_STREAM_ID);
        Assert.assertFalse(StormUtil.isTickTuple(bogusStreamTuple));

        // Create a command tuple
        Tuple commandTuple = TupleUtil
                .mockCommandTuple(TopologyCommand.SHUTDOWN);
        Assert.assertFalse(StormUtil.isTickTuple(commandTuple));

        // Create a generic data tuple.
        Tuple dataTuple = TupleUtil.mockDataTuple();
        Assert.assertFalse(StormUtil.isTickTuple(dataTuple));
    }

    /**
     * Test the shutdown tuple test.
     */
    @Test
    public final void testIsShutdownTuple() {

        // Try a tick tuple.
        Tuple tickTuple = TupleUtil.mockTickTuple();
        Assert.assertFalse(StormUtil.isShutdownTuple(tickTuple));

        // Create a command tuple
        Tuple commandTuple = TupleUtil
                .mockCommandTuple(TopologyCommand.SHUTDOWN);
        Assert.assertTrue(StormUtil.isShutdownTuple(commandTuple));

        // Create a different command tuple
        Tuple bogusCommandTuple = TupleUtil.mockCommandTuple("random_command");
        Assert.assertFalse(StormUtil.isShutdownTuple(bogusCommandTuple));

        // Create a null command tuple
        Tuple emptyCommandTuple = TupleUtil.mockCommandTuple("");
        Assert.assertFalse(StormUtil.isShutdownTuple(emptyCommandTuple));

        // Create a generic data tuple.
        Tuple dataTuple = TupleUtil.mockDataTuple();
        Assert.assertFalse(StormUtil.isShutdownTuple(dataTuple));
    }

    /**
     * Ensure the constructor is private.
     *
     * @throws java.lang.Exception Tests throw exceptions.
     */
    @Test
    public final void testConstructorIsPrivate() throws Exception {
        Constructor<StormUtil> constructor = StormUtil.class
                .getDeclaredConstructor();
        Assert.assertTrue(Modifier.isPrivate(constructor.getModifiers()));

        // Override the private constructor and create an instance
        constructor.setAccessible(true);
        StormUtil util = constructor.newInstance();
        Assert.assertNotNull(util);
    }
}

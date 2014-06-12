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

import io.dataplay.UnitTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Arrays;

/**
 * Unit test for our stream constants.
 */
@Category(UnitTest.class)
public class StreamTest {

    /**
     * Assert the existence of our system constants.
     *
     * @throws Exception Tests throw exceptions.
     */
    @Test
    public final void testSystemConstants() throws Exception {
        Assert.assertNotNull(Stream.BOLT_MANAGEMENT);
        Assert.assertEquals("bolt_manager", Stream.BOLT_MANAGEMENT.getName());
        Assert.assertTrue(
                Stream.BOLT_MANAGEMENT.getFields().toList().containsAll(
                        Arrays.asList("command")
                )
        );

        Assert.assertNotNull(Stream.STATUS);
        Assert.assertEquals("worker_status", Stream.STATUS.getName());
        Assert.assertTrue(
                Stream.STATUS.getFields().toList().containsAll(
                        Arrays.asList("componentId", "threadId", "state")
                )
        );
    }
}

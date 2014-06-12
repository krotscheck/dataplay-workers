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

import io.dataplay.test.UnitTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;

/**
 * Unit test for our stream constants.
 */
@Category(UnitTest.class)
public class TopologyCommandTest {

    /**
     * Ensure the constructor is private.
     *
     * @throws java.lang.Exception Tests throw exceptions.
     */
    @Test
    public final void testConstructorIsPrivate() throws Exception {
        Constructor<TopologyCommand> constructor = TopologyCommand.class
                .getDeclaredConstructor();
        Assert.assertTrue(Modifier.isPrivate(constructor.getModifiers()));

        // Override the private constructor and create an instance
        constructor.setAccessible(true);
        TopologyCommand util = constructor.newInstance();
        Assert.assertNotNull(util);
    }
}

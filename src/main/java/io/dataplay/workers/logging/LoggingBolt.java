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

package io.dataplay.workers.logging;

import io.dataplay.workers.AbstractBolt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import backtype.storm.tuple.Tuple;

/**
 * This simple logging bolt simple logs the contents of a tuple before sending
 * it on its way. Useful for debugging.s
 *
 * @author Michael Krotscheck
 */
public final class LoggingBolt extends AbstractBolt {

    /**
     * Logger instance.
     */
    private Logger logger = LoggerFactory.getLogger(LoggingBolt.class);

    /**
     * Get the active logger for this bolt.
     *
     * @return The current logger.
     */
    public Logger getLogger() {
        return logger;
    }

    /**
     * Set the active logger for this bolt.
     *
     * @param newLogger The logger to use in this bolt.
     */
    public void setLogger(final Logger newLogger) {
        this.logger = newLogger;
    }

    /**
     * Calculate the schema emitted by this bolt.
     *
     * @param parentSchema A list of parent schema.
     * @return The merged schema.
     */
    @Override
    public Map<String, String> calculateSchema(final List<Map<String, String>>
                                                       parentSchema) {
        return mergeSchema(parentSchema);
    }

    /**
     * Logs the content of a tuple, then sends it on.
     *
     * @param tuple The tuple to log.
     */
    @Override
    protected void process(final Tuple tuple) {
        // Log the tuple.
        logger.info(tuple.toString());

        // Emit the tuple.
        emit(tuple, tuple.getValues());
    }


    /**
     * Logs a tick tuple.
     */
    @Override
    protected void tick() {
        // Log that a tick was received.
        logger.info("Tick");
    }

    /**
     * Returns whether this bolt's configuration is valid.
     *
     * @return True, the configuration is always valid.
     */
    @Override
    public Boolean isValid() {
        return logger != null;
    }
}

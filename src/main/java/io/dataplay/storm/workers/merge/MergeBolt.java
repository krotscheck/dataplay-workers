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

import io.dataplay.storm.workers.AbstractBolt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

/**
 * The merge bolt allows us to combine two data streams. It doesn't attempt to
 * make intelligent guesses about what field needs to be added to which outgoing
 * tuple, it merely asserts that there is now a merged fields and sends the
 * tuples on their way.
 *
 * @author Michael Krotscheck
 */
public final class MergeBolt extends AbstractBolt {

    /**
     * Logger instance.
     */
    private Logger logger = LoggerFactory.getLogger(MergeBolt.class);

    /**
     * The merge bolt does nothing on tick.
     */
    @Override
    protected void tick() {
        logger.debug("Tick");
    }

    /**
     * The merge bolt fields is calculated as an aggregate merge of all incoming
     * streams.
     *
     * @param parentFields A list of parent fields.
     */
    @Override
    public void calculateFields(final List<Fields> parentFields) {
        setFields(mergeFields(parentFields));
    }

    /**
     * Combine two tuples into one set of output values, and emit that.
     *
     * @param tuple The tuple to handle.
     */
    @Override
    public void process(final Tuple tuple) {
        Fields tupleFields = tuple.getFields();
        Fields schema = getFields();

        List<Object> values = new ArrayList<>();

        // Create a new list of values
        for (String field : schema) {
            int idx = schema.fieldIndex(field);

            if (tupleFields.contains(field)) {
                values.add(idx, tuple.getValueByField(field));
            } else {
                values.add(idx, "");
            }
        }

        emit(tuple, values);
    }

    /**
     * The merge bolt is always valid.
     *
     * @return True: The merge bolt is always valid.
     */
    @Override
    public Boolean isValid() {
        return getFields() != null;
    }
}

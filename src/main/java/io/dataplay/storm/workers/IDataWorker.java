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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;

import backtype.storm.tuple.Fields;

/**
 * This is the base interface to which all spouts and bolts in our library must
 * adhere. Its intent is to expose methods by which a spout/bolt may be asked to
 * determine whether its configuration is valid.
 *
 * @author Michael Krotscheck
 */
// We're ignoring common bolt/spout properties here, so that we can use jackson
// de/serialization to generate configuration objects.
@JsonIgnoreProperties(value = {"valid", "registryKey",
        "componentConfiguration", "schema" }, ignoreUnknown = true)
public interface IDataWorker {

    /**
     * This method reveals whether the present configuration of this worker is
     * valid.
     *
     * @return True if the configuration is valid, otherwise False.
     */
    Boolean isValid();

    /**
     * This method asks an instance to determine its emitted schema.
     */
    void calculateFields();

    /**
     * This method asks an instance to determine its EMITTED schema based on a
     * collection of parent schemas. This method can be used if we know the bolt
     * in question has multiple parents in the topology.
     *
     * @param parentSchema A list of parent schemae, describing the format of
     *                     tuples which this worker should expect to receive.
     */
    void calculateFields(List<Fields> parentSchema);

    /**
     * This method asks an instance to determine its EMITTED schema based on a
     * single parent schema. This method can be used if we know the bolt in
     * question only has one parent in the topology.
     *
     * @param parentSchema A single parent schema, describing the format of
     *                     tuples which this worker should expect to receive.
     */
    void calculateFields(Fields parentSchema);

    /**
     * Get the fields schema.
     *
     * @return The current configured schema.
     */
    Fields getFields();

    /**
     * Set the fields Schema.
     *
     * @param fields A schema to set.
     */
    void setFields(Fields fields);
}

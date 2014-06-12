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

package io.dataplay;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;
import java.util.Map;

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
     *
     * @return A schema, calculated based on the present worker's configuration.
     */
    Map<String, String> calculateSchema();

    /**
     * This method asks an instance to determine its EMITTED schema based on a
     * collection of parent schemas. This method can be used if we know the bolt
     * in question has multiple parents in the topology.
     *
     * @param parentSchema A list of parent schemae, describing the format of
     *                     tuples which this worker should expect to receive.
     * @return A schema, calculated based on the present worker's configuration
     * and a list of input schema.
     */
    Map<String, String> calculateSchema(
            List<Map<String, String>> parentSchema);

    /**
     * This method asks an instance to determine its EMITTED schema based on a
     * single parent schema. This method can be used if we know the bolt in
     * question only has one parent in the topology.
     *
     * @param parentSchema A single parent schema, describing the format of
     *                     tuples which this worker should expect to receive.
     * @return A schema, calculated based on the present worker's configuration
     * and a single input schema.
     */
    Map<String, String> calculateSchema(
            Map<String, String> parentSchema);

    /**
     * Get the schema.
     *
     * @return The current configured schema.
     */
    Map<String, String> getSchema();

    /**
     * Set the schema.
     *
     * @param schema A schema to set.
     */
    void setSchema(Map<String, String> schema);
}

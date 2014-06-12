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
import io.dataplay.storm.util.StormUtil;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;

/**
 * This implementation abstracts away the management of the outputCollector,
 * simplifying the implementation of other bolts. In most cases, you should
 * extend this bolt directly to implement your own functionality.
 *
 * @author Michael Krotscheck
 */
public abstract class AbstractBolt extends BaseRichBolt implements IDataWorker {

    /**
     * Our schema.
     */
    private Map<String, String> schema;

    /**
     * The output collector.
     */
    private OutputCollector boltOutputCollector;

    /**
     * Configuration.
     */
    private Map boltConfig;

    /**
     * Return the output collector.
     *
     * @return The current bolt output collector.
     */
    public final OutputCollector getBoltOutputCollector() {
        return boltOutputCollector;
    }

    /**
     * The configuration.
     *
     * @return Return the bolt configuration.
     */
    public final Map getBoltConfig() {
        return boltConfig;
    }

    /**
     * Return the topology context.
     *
     * @return The current topology context.
     */
    public final TopologyContext getContext() {
        return context;
    }

    /**
     * The passed topology context.
     */
    private TopologyContext context;

    /**
     * Prepares this bolt for execution.
     *
     * @param config          The storm cluster configuration.
     * @param topologyContext The topology context & configuration in which this
     *                        bolt is operating.
     * @param outputCollector The output collector where this bolt should send
     *                        its content.
     */
    @Override
    public final void prepare(final Map config, final TopologyContext
            topologyContext, final OutputCollector outputCollector) {
        // TODO(krotscheck): Add Metrics Hooks.
        boltOutputCollector = outputCollector;
        boltConfig = config;
        context = topologyContext;
    }

    /**
     * Return the data schema for this spout.
     *
     * @return The schema for this bolt.
     */
    public final Map<String, String> getSchema() {
        return schema;
    }

    /**
     * Set the data schema for this spout.
     *
     * @param newSchema Set the new schema for this bolt.
     */
    public final void setSchema(final Map<String, String> newSchema) {
        this.schema = newSchema;
    }

    /**
     * Declares the output fields for this bolt during topology initialization.
     * They are derived from the configured schema.
     *
     * @param outputFieldsDeclarer The declarer which receives the configured
     *                             schema.
     */
    @Override
    public final void declareOutputFields(final OutputFieldsDeclarer
                                                  outputFieldsDeclarer) {
        // Declare the default stream with our configured schema
        Map<String, String> currentSchema = getSchema();
        List<String> fields = new ArrayList<>();
        fields.addAll(currentSchema.keySet());
        outputFieldsDeclarer.declareStream(
                Utils.DEFAULT_STREAM_ID,
                new Fields(fields)
        );

        // Declare the bolt status stream.
        outputFieldsDeclarer.declareStream(
                Stream.STATUS.getName(),
                Stream.STATUS.getFields());
    }

    /**
     * Emit a tuple anchored to several different input tuples with a given
     * string ID.
     *
     * @param streamId The ID of the stream to emit to.
     * @param anchors  A list of anchors related to the emitted tuple.
     * @param tuple    The emitted tuple.
     * @return The list of message ID's
     */
    protected final List<Integer> emit(final String streamId,
                                       final Collection<Tuple> anchors,
                                       final List<Object> tuple) {
        return boltOutputCollector.emit(streamId, anchors, tuple);
    }

    /**
     * Emit a single anchored tuple with a given string ID.
     *
     * @param streamId The ID of the stream to emit to.
     * @param anchor   The anchor to emit this tuple for.
     * @param tuple    The emitted tuple.
     * @return The list of message ID's
     */
    protected final List<Integer> emit(final String streamId,
                                       final Tuple anchor,
                                       final List<Object> tuple) {
        return boltOutputCollector.emit(streamId, anchor, tuple);
    }

    /**
     * Emit a tuple to the default stream, anchored to several different input
     * tuples.
     *
     * @param anchors A list of anchors related to the emitted tuple.
     * @param tuple   The emitted tuple.
     * @return A list of message ID's.
     */
    protected final List<Integer> emit(final Collection<Tuple> anchors,
                                       final List<Object> tuple) {
        return boltOutputCollector.emit(Utils.DEFAULT_STREAM_ID,
                anchors, tuple);
    }

    /**
     * Emit a single anchored tuple to the default stream.
     *
     * @param anchor The anchor to emit this tuple for.
     * @param tuple  The emitted tuple.
     * @return A list of message ID's.
     */
    protected final List<Integer> emit(final Tuple anchor,
                                       final List<Object> tuple) {
        return boltOutputCollector.emit(Utils.DEFAULT_STREAM_ID,
                anchor, tuple);
    }

    /**
     * This method acks a tuple.
     *
     * @param tuple The tuple to ack.
     */
    public final void ack(final Tuple tuple) {
        boltOutputCollector.ack(tuple);
    }

    /**
     * Fail a tuple.
     *
     * @param tuple The tuple to fail.
     */
    public final void fail(final Tuple tuple) {
        boltOutputCollector.fail(tuple);
    }

    /**
     * This method reports an error to the supervisor.
     *
     * @param throwable The error that occurred.
     */
    public final void reportError(final Throwable throwable) {
        boltOutputCollector.reportError(throwable);
    }

    /**
     * Calculate the data schema for this bolt.
     *
     * @return The schema.
     */
    @Override
    public final Map<String, String> calculateSchema() {
        // Bolts must have a parent schema.
        return calculateSchema(new ArrayList<Map<String, String>>());
    }

    /**
     * Calculate the data schema for this bolt, given a single parent schema.
     *
     * @param parentSchema The parent schema.
     * @return The bolt's schema.
     */
    @Override
    public final Map<String, String> calculateSchema(final Map<String,
            String> parentSchema) {
        List<Map<String, String>> parentSchemae = new ArrayList<>();
        parentSchemae.add(parentSchema);
        return calculateSchema(parentSchemae);
    }

    /**
     * Calculate the data schema for this bolt, given several parent schema.
     *
     * @param parentSchema A list of parent schema.
     * @return The bolt's schema.
     */
    @Override
    public abstract Map<String, String> calculateSchema(final List<Map<String,
            String>> parentSchema);

    /**
     * Helper method that calculates the schema for this bolt by simply merging
     * the provided parent schema.
     *
     * @param mergeSchema A list of schema to merge.
     * @return A merged schema.
     */
    protected final Map<String, String> mergeSchema(
            final List<Map<String, String>> mergeSchema) {
        Map<String, String> finalSchema = new HashMap<>();

        for (Map<String, String> schemaItem : mergeSchema) {
            for (Entry<String, String> entry : schemaItem.entrySet()) {
                finalSchema.put(entry.getKey(), entry.getValue());
            }
        }

        return finalSchema;
    }

    /**
     * This method must be implemented when a tuple is handled.
     *
     * @param tuple The tuple to handle.
     */
    protected abstract void process(Tuple tuple);

    /**
     * A tick method that is invoked when a system's tick tuple is encountered.
     */
    protected abstract void tick();

    /**
     * The bolt's execution method, finalized to support the IDataWorker API.
     *
     * @param tuple The tuple to operate on.
     */
    @Override
    public final void execute(final Tuple tuple) {
        if (StormUtil.isShutdownTuple(tuple)) {
            cleanup();
        }
        if (StormUtil.isTickTuple(tuple)) {
            tick();
        } else {
            process(tuple);
        }
        ack(tuple);
    }
}

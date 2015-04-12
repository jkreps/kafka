/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.common.requests;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ProtoUtils;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.*;

public class JoinGroupResponse extends AbstractRequestResponse {
    
    private static final Schema CURRENT_SCHEMA = ProtoUtils.currentResponseSchema(ApiKeys.JOIN_GROUP.id);
    private static final String ERROR_CODE_KEY_NAME = "error_code";

    /**
     * Possible error code:
     *
     * TODO
     */

    private static final String GENERATION_ID_KEY_NAME = "group_generation_id";
    private static final String CONSUMER_ID_KEY_NAME = "consumer_id";
    private static final String ASSIGNMENT_KEY_NAME = "assignment";
    private static final String TOPIC_KEY_NAME = "topic";
    private static final String PARTITIONS_KEY_NAME = "partitions";
    private static final String PARTITION_KEY_NAME = "partition";
    private static final String GROUPING_ID_KEY_NAME = "grouping_id";

    public static final int UNKNOWN_GENERATION_ID = -1;
    public static final String UNKNOWN_CONSUMER_ID = "";

    private final short errorCode;
    private final int generationId;
    private final String consumerId;
    private final Map<Integer, List<TopicPartition>> assignments;

    public JoinGroupResponse(short errorCode, int generationId, String consumerId, Map<Integer, List<TopicPartition>> assignments) {
        super(new Struct(CURRENT_SCHEMA));

        struct.set(ERROR_CODE_KEY_NAME, errorCode);
        struct.set(GENERATION_ID_KEY_NAME, generationId);
        struct.set(CONSUMER_ID_KEY_NAME, consumerId);
        int i = 0;
        Struct[] assigned = new Struct[assignments.size()];
        for (Map.Entry<Integer, List<TopicPartition>> entry: assignments.entrySet()) {
            Struct grouping = struct.instance(ASSIGNMENT_KEY_NAME);
            grouping.set(GROUPING_ID_KEY_NAME, entry.getKey());
            Struct[] partitions = new Struct[entry.getValue().size()];
            int j = 0;
            for (TopicPartition part: entry.getValue()) {
                partitions[j] = grouping.instance(PARTITIONS_KEY_NAME);
                partitions[j].set(TOPIC_KEY_NAME, part.topic());
                partitions[j].set(PARTITION_KEY_NAME, part.partition());
                j++;
            }
            grouping.set(PARTITIONS_KEY_NAME, partitions);
            assigned[i++] = grouping;
        }
        struct.set(ASSIGNMENT_KEY_NAME, assigned);
        this.errorCode = errorCode;
        this.generationId = generationId;
        this.consumerId = consumerId;
        this.assignments = assignments;
    }

    public JoinGroupResponse(short errorCode) {
        this(errorCode, UNKNOWN_GENERATION_ID, UNKNOWN_CONSUMER_ID, Collections.<Integer, List<TopicPartition>>emptyMap());
    }

    public JoinGroupResponse(Struct struct) {
        super(struct);
        this.assignments = new HashMap<Integer, List<TopicPartition>>();
        for (Object topicDataObj : struct.getArray(ASSIGNMENT_KEY_NAME)) {
            Struct topicData = (Struct) topicDataObj;
            Integer grouping = topicData.getInt(GROUPING_ID_KEY_NAME);
            Object[] partitions = (Object[]) topicData.getArray(PARTITIONS_KEY_NAME);
            List<TopicPartition> partsList = new ArrayList<TopicPartition>(partitions.length);
            for (Object partObj: partitions) {
                Struct part = (Struct) partObj;
                String topic = part.getString(TOPIC_KEY_NAME);
                Integer p = part.getInt(PARTITION_KEY_NAME);
                partsList.add(new TopicPartition(topic, p));
            }
            this.assignments.put(grouping, partsList);
        }
        errorCode = struct.getShort(ERROR_CODE_KEY_NAME);
        generationId = struct.getInt(GENERATION_ID_KEY_NAME);
        consumerId = struct.getString(CONSUMER_ID_KEY_NAME);
    }

    public short errorCode() {
        return errorCode;
    }

    public int generationId() {
        return generationId;
    }

    public String consumerId() {
        return consumerId;
    }

    public Map<Integer, List<TopicPartition>> assignments() {
        return assignments;
    }

    public static JoinGroupResponse parse(ByteBuffer buffer) {
        return new JoinGroupResponse((Struct) CURRENT_SCHEMA.read(buffer));
    }
}
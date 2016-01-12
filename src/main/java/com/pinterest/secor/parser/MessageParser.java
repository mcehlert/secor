/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.pinterest.secor.parser;

import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.message.Message;
import com.pinterest.secor.message.ParsedMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;

// TODO(pawel): should we offer a multi-message parser capable of parsing multiple types of
// messages?  E.g., it could be implemented as a composite trying out different parsers and using
// the one that works.  What is the performance cost of such approach?

/**
 * Message parser extracts partitions from messages.
 *
 * @author Pawel Garbacki (pawel@pinterest.com)
 */
public abstract class MessageParser {
    protected SecorConfig mConfig;
    private static final Logger LOG = LoggerFactory.getLogger(MessageParser.class);

    public MessageParser(SecorConfig config) {
        mConfig = config;
    }

    public ParsedMessage parse(Message message) throws Exception {
        String[] partitions = extractPartitions(message);
        return new ParsedMessage(message.getTopic(), message.getKafkaPartition(),
                                 message.getOffset(), message.getKafkaKey(),
                                 message.getPayload(), partitions);
    }

    public abstract String[] extractPartitions(Message payload) throws Exception;
    
    public Object getJsonFieldValue(JSONObject jsonObject) {
        Object fieldValue = null;
        String messageTimestampName = mConfig.getMessageTimestampName();
        String messageTimestampSeparator = mConfig.getMessageTimestampNameSeparator();
        if (messageTimestampSeparator != null) {
            String[] fields = messageTimestampName.split(messageTimestampSeparator);
            Object finalValue = null;
            
            for (int i=0; i < fields.length; i++) {
                if (!jsonObject.containsKey(fields[i])) {
                    LOG.warn("Could not find key {} in message", messageTimestampName);
                    break;
                }
                if (i < (fields.length -1)) {
                    jsonObject = (JSONObject) jsonObject.get(fields[i]);
                } else {
                    finalValue = jsonObject.get(fields[i]);
                }
            }
            fieldValue = finalValue;
        } else {
            fieldValue = jsonObject.get(messageTimestampName);
        }
        return fieldValue;
    }
}

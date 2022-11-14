/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.elasticsearch.util;

import org.apache.flink.annotation.Internal;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ser.impl.SimpleBeanPropertyFilter;
import com.fasterxml.jackson.databind.ser.impl.SimpleFilterProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/** JsonFilterUtils. */
@Internal
public class JsonFilterUtils {

    private static final Logger LOG = LoggerFactory.getLogger(JsonFilterUtils.class);

    public static byte[] excludeJsonField(byte[] document, String excludeKey) {
        final String[] split = excludeKey.split(",");
        ObjectMapper objectMapper = new ObjectMapper();
        SimpleFilterProvider simpleFilterProvider =
                new SimpleFilterProvider()
                        .addFilter("non-name", SimpleBeanPropertyFilter.serializeAllExcept(split));
        objectMapper.setFilterProvider(simpleFilterProvider);
        Object o;
        try {
            o = objectMapper.readValue(document, Object.class);
            return objectMapper.writeValueAsBytes(o);
        } catch (IOException e) {
            LOG.error("json filter error, json: {}, err_message: {}", new String(document), e);
            return null;
        }
    }
}

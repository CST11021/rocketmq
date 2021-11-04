/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.example.rpc;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class RequestProducer {

    public static void main(String[] args) throws Exception {
        String producerGroup = "please_rename_unique_group_name";
        String topic = "RequestTopic";
        long ttl = 3000;

        DefaultMQProducer producer = new DefaultMQProducer(producerGroup);
        producer.setNamesrvAddr("127.0.0.1:9876");
        producer.start();

        InputStreamReader is = new InputStreamReader(System.in);
        BufferedReader br = new BufferedReader(is);
        while (true) {
            String txt = br.readLine();
            if (StringUtils.isNotEmpty(txt)) {
                Message msg = new Message(topic, "", txt.getBytes(RemotingHelper.DEFAULT_CHARSET));

                long begin = System.currentTimeMillis();
                Message retMsg = producer.request(msg, ttl);
                long cost = System.currentTimeMillis() - begin;
                System.out.printf("request to <%s> cost: %d replyMessage: %s %n", topic, cost, retMsg);
            }
        }

        // producer.shutdown();
    }
}

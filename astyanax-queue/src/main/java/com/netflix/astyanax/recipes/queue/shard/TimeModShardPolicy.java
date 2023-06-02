/**
 * Copyright 2013 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.astyanax.recipes.queue.shard;

import com.netflix.astyanax.recipes.queue.Message;
import com.netflix.astyanax.recipes.queue.MessageQueueMetadata;

/**
 * Sharding based on time.  This policy assumes that the 
 * next trigger time in the message has a 'unique' incrementing
 * lower bits.
 * 
 * @author elandau
 *
 */
public class TimeModShardPolicy implements ModShardPolicy {
    private static TimeModShardPolicy instance = new TimeModShardPolicy();

    public static ModShardPolicy getInstance() {
        return instance;
    }

    @Override
    public int getMessageShard(Message message, MessageQueueMetadata settings) {
        return (int) (message.getTokenTime() % settings.getShardCount());
    }
}

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
package com.netflix.astyanax.recipes.queue;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;

import com.netflix.astyanax.annotations.Component;
import com.netflix.astyanax.util.TimeUUIDUtils;

public class MessageQueueEntry {
    private static final String ID_DELIMITER = ":";
            
    /**
     * Type of column.  
     * 0 - Lock
     * 1 - Queue item
     */
    @Component(ordinal=0)
    private Byte type;
    
    @Component(ordinal=1)
    private Byte priority;
    
    /**
     * Time when item is to be processed
     */
    @Component(ordinal=2)
    private UUID timestamp;
    
    /**
     * Random number to help ensure each entry is unique
     */
    @Component(ordinal=3)
    private UUID random;

    /**
     * 
     */
    @Component(ordinal=4)
    private Byte state;
    
    public MessageQueueEntry() {
    }
    
    public MessageQueueEntry(String id) {
        String[] parts = StringUtils.split(id, ID_DELIMITER);
        if (parts.length != 5) {
            throw new RuntimeException("Invalid message ID.  Expection <type>:<priority>:<timestamp>:<random>:<state> but got " + id);
        }
        
        type      = Byte.parseByte(parts[0]);
        priority  = Byte.parseByte(parts[1]);
        timestamp = UUID.fromString (parts[2]);
        random    = UUID.fromString (parts[3]);
        state     = Byte.parseByte(parts[4]);
    }
    
    private MessageQueueEntry(MessageQueueEntryType type, byte priority, UUID timestamp, UUID random, MessageQueueEntryState state) {
        super();
        this.type       = (byte)type.ordinal();
        this.priority   = 0;
        this.timestamp  = timestamp;
        this.state      = (byte)state.ordinal();
        if (random == null) {
            this.random = TimeUUIDUtils.getUniqueTimeUUIDinMicros();
        }
        else {
            this.random = random;
        }
    }
    
    public static MessageQueueEntry newLockEntry(MessageQueueEntryState state) {
        return new MessageQueueEntry(MessageQueueEntryType.Lock, (byte)0, TimeUUIDUtils.getUniqueTimeUUIDinMicros(), null, state);
    }
    
    public static MessageQueueEntry newLockEntry(UUID timestamp, MessageQueueEntryState state) {
        return new MessageQueueEntry(MessageQueueEntryType.Lock, (byte)0, timestamp, null, state);
    }
    
    public static MessageQueueEntry newMetadataEntry() {
        return new MessageQueueEntry(MessageQueueEntryType.Metadata, (byte)0, null, TimeUUIDUtils.getMicrosTimeUUID(0), MessageQueueEntryState.None);
    }
    
    public static MessageQueueEntry newMessageEntry(byte priority, UUID timestamp, MessageQueueEntryState state) {
        return new MessageQueueEntry(MessageQueueEntryType.Message,  priority, timestamp, null, state);
    }
    
    public static MessageQueueEntry newBusyEntry(Message message) {
        return new MessageQueueEntry(MessageQueueEntryType.Message, (byte)message.getPriority(), message.getToken(), message.getRandom(), MessageQueueEntryState.Busy);
    }
    
    public static MessageQueueEntry fromMetadata(MessageMetadataEntry meta) {
        String[] parts = StringUtils.split(meta.getName(), "$");
        return new MessageQueueEntry(parts[1]);
    }

    public MessageQueueEntryType getType() {
        return MessageQueueEntryType.values()[type];
    }

    public UUID getTimestamp() {
        return timestamp;
    }
    
    public long getTimestamp(TimeUnit units) {
        return units.convert(TimeUUIDUtils.getMicrosTimeFromUUID(timestamp), TimeUnit.MICROSECONDS);
    }

    public MessageQueueEntryState getState() {
        return MessageQueueEntryState.values()[state];
    }

    public byte getPriority() {
        return priority;
    }
    
    public void setType(Byte type) {
        this.type = type;
    }

    public void setTimestamp(UUID timestamp) {
        this.timestamp = timestamp;
    }

    public void setState(Byte state) {
        this.state = state;
    }

    public void setPriorty(Byte priority) {
        this.priority = priority;
    }

    public String getMessageId() {
        return String.valueOf(type) + ID_DELIMITER + priority + ID_DELIMITER + timestamp.toString() + ID_DELIMITER + random.toString() + ID_DELIMITER + state;
        
    }
    
    public UUID getRandom() {
        return random;
    }

    public void setRandom(UUID random) {
        this.random = random;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("MessageQueueEntry [");
        sb.append(  "type="      + MessageQueueEntryType.values()[type]);
        sb.append(", priority=").append(priority);
        if (timestamp != null) {
            
                    sb.append(", timestamp=").append(timestamp).append("(").append(TimeUUIDUtils.getMicrosTimeFromUUID(timestamp)).append(")");
        }
        sb.append(", random=").append(random);
        sb.append(", state="     + MessageQueueEntryState.values()[state]);
        sb.append("]");
        return sb.toString();
    }
    
    
}
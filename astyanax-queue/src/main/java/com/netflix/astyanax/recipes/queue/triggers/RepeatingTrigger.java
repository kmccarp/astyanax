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
package com.netflix.astyanax.recipes.queue.triggers;

import java.util.concurrent.TimeUnit;


public class RepeatingTrigger extends AbstractTrigger {
    public static class Builder {
        private RepeatingTrigger trigger = new RepeatingTrigger();
        
        public Builder withInterval(long interval, TimeUnit units) {
            trigger.interval = TimeUnit.MILLISECONDS.convert(interval, units);
            return this;
        }
        
        public Builder withDelay(long delay, TimeUnit units) {
            trigger.delay = TimeUnit.MILLISECONDS.convert(delay,  units);
            return this;
        }
        
        public Builder withRepeatCount(long repeatCount) {
            trigger.repeatCount = repeatCount;
            return this;
        }
        
        public Builder withEndTime(long endTime, TimeUnit units) {
            trigger.endTime = TimeUnit.MILLISECONDS.convert(endTime, units);
            return this;
        }
        
        public RepeatingTrigger build() {
            if (trigger.delay != null) {
                trigger.setTriggerTime(System.currentTimeMillis() + trigger.delay);
            } else {
                trigger.setTriggerTime(System.currentTimeMillis());
            }
            return trigger;
        }
    }

    private Long delay             ;   // In millseconds
    private long interval;   // In milliseconds
    private Long repeatCount       ;   // Repeat count
    private long endTime;
    
    @Override
    public Trigger nextTrigger() {
        if (repeatCount != null && getExecutionCount()+1 >= repeatCount) {
            return null;
        }
        
        long currentTime = System.currentTimeMillis();
        long nextTime    = getTriggerTime() + interval;
        if (endTime != 0 && (nextTime > endTime || currentTime > endTime)) {
            return null;
        }
        
        RepeatingTrigger next = new RepeatingTrigger();
        next.delay        = delay;
        next.interval     = interval;
        next.repeatCount  = repeatCount;
        next.setExecutionCount(getExecutionCount() + 1);
        
        // TODO: Handle missed or delayed execution
        next.setTriggerTime(getTriggerTime() + interval);

        return next;
    }

    public Long getDelay() {
        return delay;
    }

    public long getInterval() {
        return interval;
    }

    public Long getRepeatCount() {
        return repeatCount;
    }

    public long getEndTime() {
        return endTime;
    }

    public void setDelay(long delay) {
        this.delay = delay;
    }

    public void setInterval(long interval) {
        this.interval = interval;
    }

    public void setRepeatCount(long repeatCount) {
        this.repeatCount = repeatCount;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    @Override
    public String toString() {
    	StringBuilder sb = new StringBuilder();
    	sb.append("RepeatingTrigger[interval=").append(interval);
        if (delay != null) {
            
                    sb.append(", delay=").append(delay);
        }
        if (repeatCount != null) {
            
                    sb.append(", repeatCount=").append(repeatCount);
        }
        if (endTime > 0) {
            
                    sb.append(", endTime=").append(endTime);
        }
    	sb.append("]");
    	return sb.toString();
    }
}

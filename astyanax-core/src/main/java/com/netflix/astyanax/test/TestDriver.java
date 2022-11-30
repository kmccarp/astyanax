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
package com.netflix.astyanax.test;

import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;

public class TestDriver {
    private int                         nThreads;
    private Supplier<Integer>           callsPerSecond;
    private ScheduledExecutorService    executor;
    private Function<TestDriver, Void>  callback;
    private volatile int                delta;
    private AtomicLong                  callbackCounter = new AtomicLong();
    private long                        iterationCount = 100;
    private long                        futuresTimeout;
    private TimeUnit                    futuresUnits = TimeUnit.MILLISECONDS;
    private ExecutorService             futuresExecutor;
    private ArrayList<Event>            events = Lists.newArrayList();              
    private long                        startTime;
    private AtomicLong                  operationCounter = new AtomicLong(0);
    
    public abstract static class Event {
        protected Function<TestDriver, Void> function;
        
        public Event(Function<TestDriver, Void> function) {
            this.function = function;
        }
        
        public abstract void addToExecutor(ScheduledExecutorService service, final TestDriver driver);
    }
    
    public static class RecurringEvent extends Event{
        private final long delay;
        private final TimeUnit units;
        
        public RecurringEvent(Function<TestDriver, Void> function, long delay, TimeUnit units) {
            super(function);
            
            this.delay = delay;
            this.units = units;
        }
        
        public void addToExecutor(ScheduledExecutorService service, final TestDriver driver) {
            service.scheduleAtFixedRate(() -> {
                function.apply(driver);
            }, delay, delay, units);
        }
    }
    
    public static class Builder {
        private TestDriver driver = new TestDriver();
        
        public Builder withThreadCount(int nThreads) {
            driver.nThreads = nThreads;
            return this;
        }
        
        public Builder withCallsPerSecondSupplier(Supplier<Integer> callsPerSecond) {
            driver.callsPerSecond = callsPerSecond;
            return this;
        }
        
        public Builder withCallback(Function<TestDriver, Void>  callback) {
            driver.callback = callback;
            return this;
        }
        
        public Builder withIterationCount(long iterationCount) {
            driver.iterationCount = iterationCount;
            return this;
        }
        
        public Builder withFutures(long timeout, TimeUnit units) {
            driver.futuresTimeout = timeout;
            driver.futuresUnits = units;
            return this;
        }
        
        public Builder withRecurringEvent(long delay, TimeUnit units, Function<TestDriver, Void> event) {
            driver.events.add(new RecurringEvent(event, delay, units));
            return this;
        }
        
        public TestDriver build() {
            driver.executor = Executors.newScheduledThreadPool(driver.nThreads + 10);
            if (driver.futuresTimeout != 0) {
                driver.futuresExecutor = Executors.newScheduledThreadPool(driver.nThreads);
            }
            return driver;
        }
    }
    
    public void start() {
        updateDelta();
        
        operationCounter.incrementAndGet();
        startTime = System.currentTimeMillis();

        for (int i = 0; i < nThreads; i++) {
            this.executor.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        Thread.sleep(new Random().nextInt(delta));
                    } catch (InterruptedException e1) {
                        throw new RuntimeException(e1);
                    }
                    long startTime = System.currentTimeMillis();
                    long nextTime  = startTime;
                    while (true) {
                        operationCounter.incrementAndGet();
                        try {
                            if (iterationCount != 0) {
                                if (callbackCounter.incrementAndGet() > iterationCount) {
                                    return;
                                }
                            }
                            
                            if (futuresTimeout == 0) {
                                callback.apply(TestDriver.this);
                            }
                            else {
                                Future<?> f = futuresExecutor.submit(new Runnable() {
                                    @Override
                                    public void run() {
                                        callback.apply(TestDriver.this);
                                    }
                                });
                                try {
                                    f.get(futuresTimeout, futuresUnits);
                                }
                                catch (Throwable t) {
                                    f.cancel(true);
                                }
                            }
                        }
                        catch (Throwable t) {
                            
                        }
                        
                        nextTime += delta;
                        long waitTime = nextTime - System.currentTimeMillis();
                        if (waitTime > 0) {
                            try {
                                Thread.sleep(waitTime);
                            } catch (InterruptedException e) {
                                return;
                            }
                        }
                    }                    
                }
            });
        }
        
        this.executor.scheduleAtFixedRate(this::updateDelta,  1, 1, TimeUnit.SECONDS);
        
        for (Event event : events) {
            event.addToExecutor(this.executor, this);
        }
    }
    
    public void updateDelta() {
        delta = 1000 * nThreads / callsPerSecond.get();
    }
    
    public void stop() {
        this.executor.shutdownNow();
    }
    
    public void await() throws InterruptedException {
        this.executor.awaitTermination(1000,  TimeUnit.HOURS);
    }
    
    public long getCallCount() {
        return callbackCounter.get();
    }
    
    public long getRuntime() {
        return System.currentTimeMillis() - startTime;
    }
    
    public long getOperationCount() {
        return operationCounter.get();
    }
    
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.bookkeeper.common.collections;

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Slf4j
public class LockOptimizedArrayBlockingQueueTest {
    @Test
    public void simpleTest() throws Exception {
        BlockingQueue<Integer> queue = new LockOptimizedArrayBlockingQueue<>(4);
        assertNull(queue.poll());
        assertEquals(4, queue.remainingCapacity());
        assertEquals("[]", queue.toString());
        assertEquals(0, queue.size());

        try {
            queue.element();
            fail("Should have thrown exception");
        } catch (NoSuchElementException e) {
            // Expected
        }

        try {
            queue.iterator();
            fail("Should have thrown exception");
        } catch (UnsupportedOperationException e) {
            // Expected
        }

        for (int i = 0; i < 100; ++i) {
            queue.add(i);
            assertEquals(1, queue.size());
            assertEquals(i, queue.take().intValue());
            assertEquals(0, queue.size());
        }

        assertTrue(queue.offer(1));
        assertEquals("[1]", queue.toString());
        assertTrue(queue.offer(2));
        assertEquals("[1, 2]", queue.toString());
        assertTrue(queue.offer(3));
        assertEquals("[1, 2, 3]", queue.toString());
        assertTrue(queue.offer(4));
        assertEquals("[1, 2, 3, 4]", queue.toString());
        assertEquals(4, queue.size());
        assertEquals(0, queue.remainingCapacity());

        try {
            queue.offer(null);
            fail("Should have thrown exception");
        } catch (NullPointerException e) {
            // Expected
        }

        assertFalse(queue.offer(5));
        assertEquals("[1, 2, 3, 4]", queue.toString());
        assertEquals(4, queue.size());
        assertEquals(0, queue.remainingCapacity());

        assertFalse(queue.offer(6, 1, TimeUnit.MILLISECONDS));
        assertEquals("[1, 2, 3, 4]", queue.toString());
        assertEquals(4, queue.size());
        assertEquals(0, queue.remainingCapacity());

        List<Integer> list = new ArrayList<>();
        queue.drainTo(list, 3);
        assertEquals(1, queue.size());
        assertEquals(Lists.newArrayList(1, 2, 3), list);
        assertEquals("[4]", queue.toString());
        assertEquals(4, queue.peek().intValue());
        assertEquals(4, queue.element().intValue());
        assertEquals(4, queue.remove().intValue());
        try {
            queue.remove();
            fail("Should have thrown exception");
        } catch (NoSuchElementException e) {
            // Expected
        }

        assertNull(queue.poll());
        assertNull(queue.poll(1, TimeUnit.MILLISECONDS));
    }

    @Test
    public void blockingTakeTest() throws Exception {
        BlockingQueue<Integer> queue = new LockOptimizedArrayBlockingQueue<>(4);

        CountDownLatch latch = new CountDownLatch(1);

        new Thread(() -> {
            try {
                int expected = 0;
                for (int i = 0; i < 100; ++i) {
                    int n = queue.take();
                    assertEquals(expected++, n);
                }
                latch.countDown();
            } catch (Exception e) {
                e.printStackTrace();
                fail();
            }
        }).start();

        int n = 0;
        for (int i = 0; i < 10; ++i) {
            for (int j = 0; j < 10; ++j) {
                queue.put(n);
                ++n;
            }

            while (!queue.isEmpty()) {
                Thread.sleep(1);
            }
        }
        latch.await();
    }

    @Test
    public void pollTimeoutTest() throws Exception {
        BlockingQueue<Integer> queue = new LockOptimizedArrayBlockingQueue<>(4);
        assertNull(queue.poll(1, TimeUnit.MILLISECONDS));
        queue.put(1);
        assertEquals(1, queue.poll(1, TimeUnit.MILLISECONDS).intValue());
        assertNull(queue.poll(0, TimeUnit.MILLISECONDS));

        queue.put(2);
        queue.put(3);
        assertEquals(2, queue.poll(1, TimeUnit.HOURS).intValue());
        assertEquals(3, queue.poll(1, TimeUnit.HOURS).intValue());
    }

    @Test
    public void pollTimeout2Test() throws Exception {
        BlockingQueue<Integer> queue = new LockOptimizedArrayBlockingQueue<>(4);
        CountDownLatch latch = new CountDownLatch(1);

        new Thread(() -> {
            try {
                queue.poll(1, TimeUnit.HOURS);

                latch.countDown();
            } catch (Exception e) {
                e.printStackTrace();
                fail();
            }
        }).start();

        Thread.sleep(100);
        queue.put(1);
        latch.await();
    }

    static class TestWriteThread extends Thread {
        private volatile boolean stop;
        private final BlockingQueue<Integer> queue;
        private final AtomicLong counter = new AtomicLong(0);

        TestWriteThread(BlockingQueue<Integer> queue) {
            this.queue = queue;
        }

        @Override
        public void run() {
            int value = 1;
            while (!stop) {
                try {
                    queue.put(value);
                    counter.incrementAndGet();
                } catch (InterruptedException e) {
                    fail();
                }
            }
        }
    }

    static class TestReadThread extends Thread {
        private volatile boolean stop;
        private final BlockingQueue<Integer> queue;
        private final AtomicLong counter = new AtomicLong(0);

        TestReadThread(BlockingQueue<Integer> queue) {
            this.queue = queue;
        }

        @Override
        public void run() {
            ArrayList<Integer> localQueue = new ArrayList<>();
            while (!stop) {
                try {
                    int items = queue.drainTo(localQueue);
                    counter.addAndGet(items);
                    localQueue.clear();
                } catch (Exception e) {
                    fail();
                }
            }
        }
    }

    //@Test
    public void testBench() throws Exception {
        for (int i = 0; i < 5; ++i) {
            benchmark();
            Thread.sleep(5000);
        }
    }
    public void benchmark() throws Exception {
        final int N = 100_000;
        BlockingQueue<Integer> queue = new LockOptimizedArrayBlockingQueue<>(N);

        //BlockingQueue<Integer> queue = new LinkedBlockingQueue<>(N);
        //BlockingQueue<Integer> queue = new ArrayBlockingQueue<>(N);

        TestWriteThread t1 = new TestWriteThread(queue);
        TestWriteThread t2 = new TestWriteThread(queue);
        TestReadThread t3 = new TestReadThread(queue);

        t1.start();
        t2.start();
        t3.start();

        Thread.sleep(20_000);
        log.info("Produce throughput {} Millions items/s, Consume throughput {} Millions items/s",
            (t1.counter.get() + t2.counter.get()) / 20 / 1e6, t3.counter.get() / 20 / 1e6);
        t1.stop = true;
        t2.stop = true;
        t3.stop = true;
    }
}

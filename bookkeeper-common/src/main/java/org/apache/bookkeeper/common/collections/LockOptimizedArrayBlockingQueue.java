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

import java.util.AbstractQueue;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

public class LockOptimizedArrayBlockingQueue<T> extends AbstractQueue<T> implements BlockingQueue<T> {

    private final ReentrantLock headLock = new ReentrantLock();
    private int headIndex;
    private int tailIndex;
    private final ReentrantLock tailLock = new ReentrantLock();
    private final Condition isNotEmpty = headLock.newCondition();
    private final Condition isNotFull = tailLock.newCondition();
    private final T[] data;

    @SuppressWarnings("rawtypes")
    private static final AtomicIntegerFieldUpdater<LockOptimizedArrayBlockingQueue> SIZE_UPDATER =
        AtomicIntegerFieldUpdater.newUpdater(LockOptimizedArrayBlockingQueue.class, "size");
    private volatile int size = 0;

    @SuppressWarnings("unchecked")
    public LockOptimizedArrayBlockingQueue(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity should greater than 0");
        }
        this.data = (T[]) new Object[capacity];
        headIndex = 0;
        tailIndex = 0;
    }

    @Override
    public T remove() {
        T item = poll();
        if (item == null) {
            throw new NoSuchElementException();
        }

        return item;
    }

    @Override
    public T poll() {
        headLock.lock();
        boolean wasFull = false;
        T item;
        try {
            if (SIZE_UPDATER.get(this) > 0) {
                item = data[headIndex];
                data[headIndex] = null;
                if (++headIndex == data.length) {
                    headIndex = 0;
                }
                if (SIZE_UPDATER.getAndDecrement(this) == data.length) {
                    wasFull = true;
                }
            } else {
                return null;
            }
        } finally {
            headLock.unlock();
        }

        if (wasFull) {
            tailLock.lock();
            try {
                isNotFull.signal();
            } finally {
                tailLock.unlock();
            }
        }

        return item;
    }

    @Override
    public T element() {
        T item = peek();
        if (item == null) {
            throw new NoSuchElementException();
        }

        return item;
    }

    @Override
    public T peek() {
        headLock.lock();
        try {
            if (SIZE_UPDATER.get(this) > 0) {
                return data[headIndex];
            } else {
                return null;
            }
        } finally {
            headLock.unlock();
        }
    }

    @Override
    public boolean offer(T e) {
        checkNotNull(e);
        tailLock.lock();
        boolean wasEmpty = false;
        try {
            if (SIZE_UPDATER.get(this) == data.length) {
                return false;
            } else {
                data[tailIndex] = e;
                if (++tailIndex == data.length) {
                    tailIndex = 0;
                }
                if (SIZE_UPDATER.getAndIncrement(this) == 0) {
                    wasEmpty = true;
                }
            }
        } finally {
            tailLock.unlock();
        }

        if (wasEmpty) {
            headLock.lock();
            try {
                isNotEmpty.signal();
            } finally {
                headLock.unlock();
            }
        }
        return true;
    }

    @Override
    public void put(T e) throws InterruptedException {
        checkNotNull(e);
        tailLock.lockInterruptibly();
        boolean wasEmpty = false;
        try {
            while (SIZE_UPDATER.get(this) == data.length) {
                isNotFull.await();
            }

            data[tailIndex] = e;
            if (++tailIndex == data.length) {
                tailIndex = 0;
            }
            if (SIZE_UPDATER.getAndIncrement(this) == 0) {
                wasEmpty = true;
            }
        } finally {
            tailLock.unlock();
        }

        if (wasEmpty) {
            headLock.lock();
            try {
                isNotEmpty.signal();
            } finally {
                headLock.unlock();
            }
        }

    }

    @Override
    public boolean add(T e) {
        return super.add(e);
    }

    @Override
    public boolean offer(T e, long timeout, TimeUnit unit) throws InterruptedException {
        checkNotNull(e);
        long nanos = unit.toNanos(timeout);
        tailLock.lockInterruptibly();
        boolean wasEmpty = false;
        try {
            while (SIZE_UPDATER.get(this) == data.length) {
                if (nanos <= 0) {
                    return false;
                }
                nanos = isNotFull.awaitNanos(nanos);
            }

            data[tailIndex] = e;
            if (++tailIndex == data.length) {
                tailIndex = 0;
            }
            if (SIZE_UPDATER.getAndIncrement(this) == 0) {
                wasEmpty = true;
            }
        } finally {
            tailLock.unlock();
        }

        if (wasEmpty) {
            headLock.lock();
            try {
                isNotEmpty.signal();
            } finally {
                headLock.unlock();
            }
        }
        return true;
    }

    @Override
    public T take() throws InterruptedException {
        headLock.lockInterruptibly();
        boolean wasFull = false;
        T item;
        try {
            while (SIZE_UPDATER.get(this) == 0) {
                isNotEmpty.await();
            }

            item = data[headIndex];
            data[headIndex] = null;
            if (++headIndex == data.length) {
                headIndex = 0;
            }
            if (SIZE_UPDATER.getAndDecrement(this) == data.length) {
                wasFull = true;
            }
        } finally {
            headLock.unlock();
        }

        if (wasFull) {
            tailLock.lock();
            try {
                isNotFull.signal();
            } finally {
                tailLock.unlock();
            }
        }
        return item;
    }

    @Override
    public T poll(long timeout, TimeUnit unit) throws InterruptedException {
        headLock.lockInterruptibly();
        boolean wasFull = false;
        T item;
        try {
            long nanos = unit.toNanos(timeout);
            while (SIZE_UPDATER.get(this) == 0) {
                if (nanos <= 0) {
                    return null;
                }
                nanos = isNotEmpty.awaitNanos(nanos);
            }

            item = data[headIndex];
            data[headIndex] = null;
            if (++headIndex == data.length) {
                headIndex = 0;
            }
            if (SIZE_UPDATER.getAndDecrement(this) == data.length) {
                wasFull = true;
            }
        } finally {
            headLock.unlock();
        }

        if (wasFull) {
            tailLock.lock();
            try {
                isNotFull.signal();
            } finally {
                tailLock.unlock();
            }
        }
        return item;
    }

    @Override
    public int remainingCapacity() {
        return data.length - SIZE_UPDATER.get(this);
    }

    @Override
    public int drainTo(Collection<? super  T> c) {
        return drainTo(c, Integer.MAX_VALUE);
    }

    @Override
    public int drainTo(Collection<? super T> c, int maxElements) {
        headLock.lock();
        boolean wasFull = false;
        int drainedItems = 0;

        try {
            int size = SIZE_UPDATER.get(this);
            while (size > 0 && drainedItems < maxElements) {
                T item = data[headIndex];
                data[headIndex] = null;
                c.add(item);

                if (++headIndex == data.length) {
                    headIndex = 0;
                }
                --size;
                ++drainedItems;
            }

            if (SIZE_UPDATER.getAndAdd(this, -drainedItems) == data.length) {
                wasFull = true;
            }
        } finally {
            headLock.unlock();
        }

        if (wasFull) {
            tailLock.lock();
            try {
                isNotFull.signalAll();
            } finally {
                tailLock.unlock();
            }
        }

        return drainedItems;
    }

    @Override
    public void clear() {
        headLock.lock();
        boolean wasFull = false;

        try {
            int size = SIZE_UPDATER.get(this);

            for (int i = 0; i < size; ++i) {
                data[headIndex] = null;
                if (++headIndex == data.length) {
                    headIndex = 0;
                }
            }

            if (SIZE_UPDATER.getAndAdd(this, -size) == data.length) {
                wasFull = true;
            }
        } finally {
            headLock.unlock();
        }

        if (wasFull) {
            tailLock.lock();
            try {
                isNotFull.signalAll();
            } finally {
                tailLock.unlock();
            }
        }
    }

    @Override
    public boolean remove(Object o) {
        if (o == null) {
            return false;
        }

        tailLock.lock();
        headLock.lock();
        boolean wasFull = SIZE_UPDATER.get(this) == data.length;

        try {
            int index = this.headIndex;
            int size = this.size;

            for (int i = 0; i < size; ++i) {
                T item = data[index];

                if (Objects.equals(item, o)) {
                    remove(index);
                    if (wasFull) {
                        isNotFull.signal();
                    }
                    return true;
                }

                if (++index == data.length) {
                    index = 0;
                }
            }
        } finally {
            headLock.unlock();
            tailLock.unlock();
        }
        return false;
    }

    private void remove(int index) {
        int tailIndex = this.tailIndex;

        if (index < tailIndex) {
            System.arraycopy(data, index + 1, data, index, tailIndex - index - 1);
            this.tailIndex--;
        } else {
            System.arraycopy(data, index + 1, data, index, data.length - index - 1);
            data[data.length - 1] = data[0];
            if (tailIndex > 0) {
                System.arraycopy(data, 1, data, 0, tailIndex);
                this.tailIndex--;
            } else {
                this.tailIndex = data.length - 1;
            }
        }

        if (tailIndex > 0) {
            data[tailIndex - 1] = null;
        } else {
            data[data.length - 1] = null;
        }

        SIZE_UPDATER.decrementAndGet(this);
    }

    @Override
    public int size() {
        return SIZE_UPDATER.get(this);
    }

    @Override
    public Iterator<T> iterator() {
        // TODO
        throw new UnsupportedOperationException();
    }

    public List<T> toList() {
        List<T> list = new ArrayList<>(size());
        forEach(list::add);
        return list;
    }

    @Override
    public void forEach(Consumer<? super T> action) {
        tailLock.lock();
        headLock.lock();

        try {
            int headIndex = this.headIndex;
            int size = this.size;

            for (int i = 0; i < size; ++i) {
                T item = data[headIndex];
                action.accept(item);
                if (++headIndex == data.length) {
                    headIndex = 0;
                }
            }
        } finally {
            headLock.unlock();
            tailLock.unlock();
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        tailLock.lock();
        headLock.lock();
        try {
            int headIndex = this.headIndex;
            int size = SIZE_UPDATER.get(this);

            sb.append('[');
            for (int i = 0; i < size; ++i) {
                T item = data[headIndex];
                if (i > 0) {
                    sb.append(", ");
                }
                sb.append(item);
                if (++headIndex == data.length) {
                    headIndex = 0;
                }
            }
            sb.append(']');
        } finally {
            headLock.unlock();
            tailLock.unlock();
        }

        return sb.toString();
    }



    private static void checkNotNull(Object v) {
        if (v == null)
            throw new NullPointerException();
    }
}

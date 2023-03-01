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

public class StampedLockArrayBlockingQueue<T> extends AbstractQueue<T> implements BlockingQueue<T> {

    private final ReentrantLock headLock = new ReentrantLock();
    private final PaddedInt headIndex = new PaddedInt();
    private final PaddedInt tailIndex = new PaddedInt();
    private final ReentrantLock tailLock = new ReentrantLock();
    private final Condition isNotEmpty = headLock.newCondition();
    private final Condition isNotFull = tailLock.newCondition();
    private final T[] data;

    @SuppressWarnings("rawtypes")
    private static final AtomicIntegerFieldUpdater<StampedLockArrayBlockingQueue> SIZE_UPDATER =
        AtomicIntegerFieldUpdater.newUpdater(StampedLockArrayBlockingQueue.class, "size");
    private volatile int size = 0;

    @SuppressWarnings("unchecked")
    public StampedLockArrayBlockingQueue(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity should greater than 0");
        }
        this.data = (T[]) new Object[capacity];
        headIndex.value = 0;
        tailIndex.value = 0;
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
        try {
            if (SIZE_UPDATER.get(this) > 0) {
                return dequeue();
            } else {
                return null;
            }
        } finally {
            headLock.unlock();
        }
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
                return data[headIndex.value];
            } else {
                return null;
            }
        } finally {
            headLock.unlock();
        }
    }

    private void enqueue(T e) {
        data[tailIndex.value] = e;
        tailIndex.value = (tailIndex.value + 1) & (data.length - 1);
        if (SIZE_UPDATER.getAndIncrement(this) == 0) {
            isNotEmpty.signal();
        }
    }

    protected T dequeue() {
        T item = data[headIndex.value];
        data[headIndex.value] = null;
        headIndex.value = (headIndex.value + 1) & (data.length - 1);
        if (SIZE_UPDATER.decrementAndGet(this) > 0) {
            isNotEmpty.signal();
        }
        return item;
    }

    @Override
    public boolean offer(T e) {
        checkNotNull(e);
        tailLock.lock();
        try {
            if (SIZE_UPDATER.get(this) == data.length) {
                return false;
            } else {
                enqueue(e);
                return true;
            }
        } finally {
            tailLock.unlock();
        }
    }

    @Override
    public void put(T e) throws InterruptedException {
        checkNotNull(e);
        tailLock.lockInterruptibly();
        try {
            while (SIZE_UPDATER.get(this) == data.length) {
                isNotFull.await();
            }
            enqueue(e);
        } finally {
            tailLock.unlock();
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
        try {
            while (SIZE_UPDATER.get(this) == data.length) {
                if (nanos <= 0) {
                    return false;
                }
                nanos = isNotFull.awaitNanos(nanos);
            }
            enqueue(e);
            return true;
        } finally {
            tailLock.unlock();
        }
    }

    @Override
    public T take() throws InterruptedException {
        headLock.lockInterruptibly();

        try {
            while (SIZE_UPDATER.get(this) == 0) {
                isNotEmpty.await();
            }

            return dequeue();
        } finally {
            headLock.unlock();
        }
    }

    @Override
    public T poll(long timeout, TimeUnit unit) throws InterruptedException {
        headLock.lockInterruptibly();

        try {
            long nanos = unit.toNanos(timeout);
            while (SIZE_UPDATER.get(this) == 0) {
                if (nanos <= 0) {
                    return null;
                }
                nanos = isNotEmpty.awaitNanos(nanos);
            }
            return dequeue();
        } finally {
            headLock.unlock();
        }
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

        try {
            int drainedItems = 0;
            int size = SIZE_UPDATER.get(this);
            while (size > 0 && drainedItems < maxElements) {
                T item = data[headIndex.value];
                data[headIndex.value] = null;
                c.add(item);

                headIndex.value = (headIndex.value + 1) & (data.length - 1);
                --size;
                ++drainedItems;
            }

            if (SIZE_UPDATER.addAndGet(this, -drainedItems) > 0) {
                isNotEmpty.signal();
            }
            return drainedItems;
        } finally {
            headLock.unlock();
        }
    }

    @Override
    public void clear() {
        headLock.lock();

        try {
            int size = SIZE_UPDATER.get(this);

            for (int i = 0; i < size; ++i) {
                data[headIndex.value] = null;
                headIndex.value = (headIndex.value + 1) & (data.length - 1);
            }

            if (SIZE_UPDATER.addAndGet(this, -size) > 0) {
                isNotEmpty.signal();
            }
        } finally {
            headLock.unlock();
        }
    }

    @Override
    public boolean remove(Object o) {
        tailLock.lock();
        headLock.lock();

        try {
            int index = this.headIndex.value;
            int size = this.size;

            for (int i = 0; i < size; ++i) {
                T item = data[index];

                if (Objects.equals(item, o)) {
                    remove(index);
                    return true;
                }

                index = (index + 1) & (data.length - 1);
            }
        } finally {
            headLock.unlock();
            tailLock.unlock();
        }
        return false;
    }

    private void remove(int index) {
        int tailIndex = this.tailIndex.value;

        if (index < tailIndex) {
            System.arraycopy(data, index + 1, data, index, tailIndex - index - 1);
            this.tailIndex.value--;
        } else {
            System.arraycopy(data, index + 1, data, index, data.length - index - 1);
            data[data.length - 1] = data[0];
            if (tailIndex > 0) {
                System.arraycopy(data, 1, data, 0, tailIndex);
                this.tailIndex.value--;
            } else {
                this.tailIndex.value = data.length - 1;
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
            int headIndex = this.headIndex.value;
            int size = this.size;

            for (int i = 0; i < size; ++i) {
                T item = data[headIndex];
                action.accept(item);
                headIndex = (headIndex + 1) & (data.length - 1);
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
            int headIndex = this.headIndex.value;
            int size = SIZE_UPDATER.get(this);

            sb.append('[');
            for (int i = 0; i < size; ++i) {
                T item = data[headIndex];
                if (i > 0) {
                    sb.append(", ");
                }
                sb.append(item);
                headIndex = (headIndex + 1) & (data.length - 1);
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


    static final class PaddedInt {
        private int value;

        // Padding to avoid false sharing
        public volatile int pi1 = 1;
        public volatile long p1 = 1L, p2 = 2L, p3 = 3L, p4 = 4L, p5 = 5L, p6 = 6L;

        public long exposeToAvoidOptimization() {
            return pi1 + p1 + p2 + p3 + p4 + p5 + p6;
        }
    }
}

package com.mapr.demo.storm.util;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class counts objects in a sliding window fashion.
 *
 * It is designed 1) to give multiple "producer" threads write access to the counter, i.e. being able to increment
 * counts of objects, and 2) to give a single "consumer" thread read access
 * to the counter. Whenever the consumer thread performs a read operation, this class will advance the head slot of the
 * sliding window counter. This means that the consumer thread indirectly controls where writes of the producer threads
 * will go to. Also, by itself this class will not advance the head slot.
 *
 * A note for analyzing data based on a sliding window count: During the initial <code>windowLengthInSlots</code>
 * iterations, this sliding window counter will always return object counts that are equal or greater than in the
 * previous iteration. This is the effect of the counter "loading up" at the very start of its existence. Conceptually,
 * this is the desired behavior.
 *
 * To give an example, using a counter with 5 slots which for the sake of this example represent 1 minute of time each:
 *
 * <pre>
 * {@code
 * Sliding window counts of an object X over time
 *
 * Minute (timeline):
 * 1    2   3   4   5   6   7   8
 *
 * Observed counts per minute:
 * 1    1   1   1   0   0   0   0
 *
 * Counts returned by counter:
 * 1    2   3   4   4   3   2   1
 * }
 * </pre>
 *
 * As you can see in this example, for the first <code>windowLengthInSlots</code> (here: the first five minutes) the
 * counter will always return counts equal or greater than in the previous iteration (1, 2, 3, 4, 4). This initial load
 * effect needs to be accounted for whenever you want to perform analyses such as trending topics; otherwise your
 * analysis algorithm might falsely identify the object to be trending as the counter seems to observe continuously
 * increasing counts. Also, note that during the initial load phase <em>every object</em> will exhibit increasing
 * counts.
 *
 * On a high-level, the counter exhibits the following behavior: If you asked the example counter after two minutes,
 * "how often did you count the object during the past five minutes?", then it should reply
 * "I have counted it 2 times in the past five minutes", implying that it can only account for the last two of those
 * five minutes because the counter was not running before that time.
 *
 * @param <T> The type of those objects we want to count.
 */
public final class SlidingWindowCounter<T> implements Serializable {

    private static final long serialVersionUID = -2645063988768785810L;

    private final Map<T, long[]> counts = Collections.synchronizedMap(new HashMap<T, long[]>());
    private final Map<T, Integer> times = Collections.synchronizedMap(new HashMap<T, Integer>());

    private final int numSlots;

    private AtomicInteger slotCount = new AtomicInteger();

    public SlidingWindowCounter(int windowLengthInSlots) {
        if (windowLengthInSlots < 2) {
            throw new IllegalArgumentException("Window length in slots must be at least two (you requested "
                    + windowLengthInSlots + ")");
        }
        numSlots = windowLengthInSlots;
    }

    public void incrementCount(T obj) {
        int t = slotCount.get();
        int slot = t % numSlots;

        times.put(obj, t);
        long[] slots = this.counts.get(obj);
        if (slots == null) {
            slots = new long[numSlots];
            counts.put(obj, slots);
        }
        slots[slot]++;
    }

    /**
     * Return the current (total) counts of all tracked objects, then advance the window.
     *
     * Whenever this method is called, we consider the counts of the current sliding window to be available to and
     * successfully processed "upstream" (i.e. by the caller). Knowing this we will start counting any subsequent
     * objects within the next "chunk" of the sliding window.
     *
     * @return The counts as they are before slipping to the next slot.
     */
    public DatedMap<T> getCountsAdvanceWindow() {
        Map<T, Long> result = new HashMap<T, Long>();

        List<T> itemsToRemove = Lists.newArrayList();
        for (T obj : counts.keySet()) {
            long count = get(obj);
            result.put(obj, count);

            if (count == 0) {
                itemsToRemove.add(obj);
            }
        }

        for (T key : itemsToRemove) {
            counts.remove(key);
            times.remove(key);
        }

        int slot = slotCount.incrementAndGet() % numSlots;
        for (T key : counts.keySet()) {
            counts.get(key)[slot] = 0;
        }
        return new DatedMap<T>(slot, numSlots, result, times);
    }

    public long get(T obj) {
        long[] curr = counts.get(obj);
        long total = 0;
        for (long k : curr) {
            total += k;
        }
        return total;
    }

    public int age(T key) {
        Integer r = times.get(key);
        if (r == null) {
            return numSlots + 1;
        } else {
            int slot = slotCount.get() % numSlots;
            return (slot - r + numSlots - 1) % numSlots;
        }
    }

    public static class DatedMap<T> {
        private int slot;
        private Map<T, Long> data = Maps.newHashMap();
        private Map<T, Integer> times = Maps.newHashMap();
        private final int numSlots;

        public DatedMap(int slot, int numSlots, Map<T, Long> data, Map<T, Integer> times) {
            this.slot = slot;
            this.numSlots = numSlots;
            this.data = data;
            this.times = ImmutableMap.copyOf(times);
        }

        public long get(T key) {
            Long r = data.get(key);
            if (r == null) {
                return 0;
            } else {
                return r;
            }
        }

        public Set<T> keySet() {
            return data.keySet();
        }

        public int age(T key) {
            Integer r = times.get(key);
            if (r == null) {
                return numSlots + 1;
            } else {
                return (slot - r + numSlots - 1) % numSlots;
            }
        }
    }
}

package com.mapr.demo.storm.util;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Maps;
import com.google.common.collect.Multiset;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * This class provides per-slot counts of the occurrences of objects.
 * <p/>
 * It can be used, for instance, as a building block for implementing sliding window counting of objects.
 *
 * @param <T> The type of those objects we want to count.
 */
public final class SlotBasedCounter<T> implements Serializable {

    private static final long serialVersionUID = 4858185737378394432L;

    private final Map<T, long[]> counts = Maps.newHashMap();
    private final Multiset<T> zeroCountKeys = HashMultiset.create();

    private final int numSlots;

    public SlotBasedCounter(int numSlots) {
        if (numSlots <= 0) {
            throw new IllegalArgumentException("Number of slots must be greater than zero (you requested " + numSlots
                    + ")");
        }
        this.numSlots = numSlots;
    }

    public void incrementCount(T key, int slot) {
        long[] slots = this.counts.get(key);
        if (slots == null) {
            slots = new long[numSlots];
            counts.put(key, slots);
        }
        slots[slot]++;
        zeroCountKeys.remove(key);
    }

    public Map<T, Long> getCounts() {
        Map<T, Long> result = new HashMap<T, Long>();
        for (T obj : counts.keySet()) {
            result.put(obj, totalCount(obj));
        }
        return result;
    }

    private long totalCount(T obj) {
        long[] curr = counts.get(obj);
        long total = 0;
        for (long k : curr) {
            total += k;
        }
        return total;
    }

    /**
     * Reset the slot count of any tracked objects to zero for the given slot.
     * <p/>
     * <em>Implementation detail:</em> As an optimization this method will also remove any object from the counter whose
     * total count is zero after the wipe of the slot (to free up memory).  Zero count objects are retained for a window
     * length to allow our caller to propagate the zero before we delete the object.
     *
     * @param slot Which slot in the ring buffer should be cleared.
     */
    public void wipeSlot(int slot) {
        for (T key : counts.keySet()) {
            counts.get(key)[slot] = 0;

            if (totalCount(key) == 0) {
                zeroCountKeys.add(key);
            }
        }

        // if a key has been zero for some time, we forget it.  We have to keep it
        // around for a while, however, so the zero count propagates downstream.
        for (T key : zeroCountKeys.elementSet()) {
            if (zeroCountKeys.count(key) > numSlots) {
                counts.remove(key);
                zeroCountKeys.remove(key);
            }
        }
    }
}

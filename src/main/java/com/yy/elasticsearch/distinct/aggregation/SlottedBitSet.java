package com.yy.elasticsearch.distinct.aggregation;

import org.apache.lucene.util.LongBitSet;

import java.util.Arrays;

/**
 * Author colin.ke keqinwu@yy.com
 */
public class SlottedBitSet {


	private static final long DEFAULT_SLOT_NUM_BITS = 1 << 18;//262144

	private long mask;
	private int maskBitNums;
	private Slot[] slots;

	public SlottedBitSet(long numBits) {
		this(numBits, DEFAULT_SLOT_NUM_BITS);
	}

	public SlottedBitSet(long numBits, long slotSize) {
		slotSize = Long.highestOneBit(slotSize - 1) << 1; //next power of two
		mask = slotSize - 1;
		maskBitNums = (int) (Math.log(slotSize) / Math.log(2));
		slots = new Slot[(int) (numBits / slotSize) + 1];
	}

	private void growSlots(int minCapacity) {
		int oldCapacity = slots.length;
		int newCapacity = oldCapacity + (oldCapacity >> 1);
		if (newCapacity - minCapacity < 0)
			newCapacity = minCapacity;
		slots = Arrays.copyOf(slots, newCapacity);
	}

	public void set(long index) {
		int slotIndex = (int) ((index & (~mask)) >>> maskBitNums);
		if (slotIndex < 0)
			throw new OutOfMemoryError("the long value is too large which makes slotIndex(int) overflow");
		if (slotIndex >= slots.length) {
			growSlots(slotIndex + 1);
		}
		if (null == slots[slotIndex]) {
			slots[slotIndex] = new Slot(mask + 1);
		}
		slots[slotIndex].add(index & mask);
	}

	public long cardinality() {
		long sum = 0;
		for (int i = 0; i < slots.length; ++i) {
			if (null != slots[i])
				sum += slots[i].cardinality();
		}
		return sum;
	}


	private static class Slot {

		final static float DEFAULT_INIT_LOAD_FACTOR = 0.25f;

		PriorityQueue queue;
		LongBitSet bitSet;
		long numBits;

		Slot(long numBits) {
			this(numBits, DEFAULT_INIT_LOAD_FACTOR);
		}

		Slot(long numBits, float initLoadFactor) {
			queue = new PriorityQueue((int) (numBits * initLoadFactor / 32));
			this.numBits = numBits;
			bitSet = null;
		}

		public long cardinality() {
			if (null != bitSet)
				return bitSet.cardinality();
			int pre = -1;
			int sum = 0;
			for (int e = queue.pop(); e != -1; e = queue.pop()) {
				if (e == pre)
					continue;
				sum++;
				pre = e;
			}
			return sum;
		}

		public void add(long index) {
			if (null != bitSet) {
				bitSet.set(index);
				return;
			}
			if (queue.size() >= queue.capacity() - 1) {
				transferToBitSet();
				bitSet.set(index);
				return;
			}
			queue.add((int) index);
		}

		private void transferToBitSet() {
			bitSet = new LongBitSet(numBits);
			int pre = -1;
			for (int e = queue.pop(); e != -1; e = queue.pop()) {
				if (e == pre)
					continue;
				bitSet.set(e);
				pre = e;
			}
			destroyQueue();

		}

		private void destroyQueue() {
			queue.destroy();
			queue = null;
		}

	}


	private static class PriorityQueue {
		private int size = 0;
		private int[] heap;


		public PriorityQueue(int maxSize) {
			final int heapSize;
			if (0 == maxSize) {
				heapSize = 2;
			} else {
				//heap[0] is unused.
				heapSize = maxSize + 1;
			}
			this.heap = new int[heapSize];
		}

		public int add(int element) {
			if (size < heap.length) {
				size++;
				heap[size] = element;
				upHeap();
				return heap[1];
			}
			return -1;
		}

		public int pop() {
			if (size > 0) {
				int result = heap[1];
				heap[1] = heap[size];
				size--;
				downHeap();
				return result;
			} else {
				return -1;
			}
		}

		public int size() {
			return size;
		}

		public int capacity() {
			return heap.length - 1;
		}

		private void upHeap() {
			int i = size;
			int node = heap[i];
			int j = i >>> 1;
			while (j > 0 && node < heap[j]) {
				heap[i] = heap[j];       // shift parents down
				i = j;
				j = j >>> 1;
			}
			heap[i] = node;
		}

		private void downHeap() {
			int i = 1;
			int node = heap[i];
			int j = i << 1;            // find smaller child
			int k = j + 1;
			if (k <= size && heap[k] < heap[j]) {
				j = k;
			}
			while (j <= size && heap[j] < node) {
				heap[i] = heap[j];       // shift up child
				i = j;
				j = i << 1;
				k = j + 1;
				if (k <= size && heap[k] < heap[j]) {
					j = k;
				}
			}
			heap[i] = node;
		}

		public void destroy() {
			heap = null;
		}


	}

}

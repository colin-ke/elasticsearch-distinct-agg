/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.yy.elasticsearch.distinct.aggregation;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.RandomAccessOrds;
import org.apache.lucene.index.SortedNumericDocValues;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.LongObjectPagedHashMap;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.format.ValueFormatter;

import java.io.IOException;

/**
 * An aggregator that computes accurate distinct counts of unique values(the field must be set as route_id for the type).
 * <p/>
 * Author colin.ke keqinwu@yy.com
 */
public class DistinctAggregator extends NumericMetricsAggregator.SingleValue {

	private final ValuesSource valuesSource;

	private Collector collector;
	private ValueFormatter formatter;

	private LongObjectPagedHashMap<SlottedBitSet> bucketBitSets;


	public DistinctAggregator(String name, long estimatedBucketsCount, ValuesSource valuesSource, @Nullable ValueFormatter formatter, AggregationContext context, Aggregator parent) {
		super(name, estimatedBucketsCount, context, parent);
		this.valuesSource = valuesSource;
		this.formatter = formatter;
		valuesSource.setNeedsGlobalOrdinals(true);

		bucketBitSets = new LongObjectPagedHashMap<>(estimatedBucketsCount, bigArrays);

	}

	@Override
	public void setNextReader(AtomicReaderContext reader) {
		postCollectLastCollector();
		//TODO: Is there really need to create a new collector every time in setNextReader, cause we've never use the reader.
		collector = createCollector(reader);
	}

	private Collector createCollector(AtomicReaderContext reader) {

		if (valuesSource instanceof ValuesSource.Numeric) {
			if (((ValuesSource.Numeric) valuesSource).isFloatingPoint())
				throw new AggregationExecutionException("Distinct agg doesn't support the numeric field with floating point");
			return new LongValueCollector(bucketBitSets, ((ValuesSource.Numeric) valuesSource).longValues());
		}

		RandomAccessOrds globalOrd = ((ValuesSource.Bytes.WithOrdinals.FieldData) valuesSource).globalOrdinalsValues();
		return new GlobalOrdinalsCollector(globalOrd, bucketBitSets);
	}


	@Override
	public boolean shouldCollect() {
		return valuesSource != null;
	}

	@Override
	public void collect(int doc, long owningBucketOrdinal) throws IOException {
		collector.collect(doc, owningBucketOrdinal);
	}

	private void postCollectLastCollector() {
		if (collector != null) {
			try {
				collector.postCollect();
				collector.close();
			} finally {
				collector = null;
			}
		}
	}

	@Override
	protected void doPostCollection() {
		postCollectLastCollector();
	}

	@Override
	public double metric(long owningBucketOrd) {
		SlottedBitSet bitSet = bucketBitSets.get(owningBucketOrd);
		long counts = 0;
		if (null != bitSet)
			counts = bitSet.cardinality();
		return counts;
	}

	@Override
	public InternalAggregation buildAggregation(long owningBucketOrdinal) {

		SlottedBitSet bitSet = bucketBitSets.get(owningBucketOrdinal);
		long counts = 0;
		if (null != bitSet)
			counts = bitSet.cardinality();
		return new InternalDistinct(name, formatter, counts);
	}

	@Override
	public InternalAggregation buildEmptyAggregation() {
		return new InternalDistinct(name, formatter, 0);
	}

	@Override
	protected void doClose() {
		Releasables.close(collector, bucketBitSets);
		bucketBitSets = null;
	}

	private interface Collector extends Releasable {

		void collect(int doc, long bucketOrd);

		void postCollect();

	}


	private class LongValueCollector implements Collector {

		private final LongObjectPagedHashMap<SlottedBitSet> bucketBitSets;
		private final SortedNumericDocValues values;

		public LongValueCollector(LongObjectPagedHashMap<SlottedBitSet> bucketBitSets, SortedNumericDocValues values) {
			this.bucketBitSets = bucketBitSets;
			this.values = values;
		}

		@Override
		public void collect(int doc, long bucketOrd) {
			values.setDocument(doc);
			if (values.count() > 0) {
				SlottedBitSet bitSet = bucketBitSets.get(bucketOrd);
				if (null == bitSet) {
					bucketBitSets.put(bucketOrd, new SlottedBitSet(Integer.MAX_VALUE));
					bitSet = bucketBitSets.get(bucketOrd);
				}
				bitSet.set(values.valueAt(0));
			}
		}

		@Override
		public void postCollect() {

		}

		@Override
		public void close() throws ElasticsearchException {

		}
	}

	private class GlobalOrdinalsCollector implements Collector {

		private final RandomAccessOrds globalOrds;
		private final LongObjectPagedHashMap<SlottedBitSet> bucketBitSets;

		public GlobalOrdinalsCollector(RandomAccessOrds globalOrds, LongObjectPagedHashMap<SlottedBitSet> bucketBitSets) {
			this.globalOrds = globalOrds;
			this.bucketBitSets = bucketBitSets;
		}

		@Override
		public void collect(int doc, long bucketOrd) {
			globalOrds.setDocument(doc);
			if (globalOrds.cardinality() > 0) {
				SlottedBitSet bitset = bucketBitSets.get(bucketOrd);
				if (null == bitset) {
					bucketBitSets.put(bucketOrd, new SlottedBitSet(globalOrds.getValueCount()));
					bitset = bucketBitSets.get(bucketOrd);
				}
				final int numOrds = globalOrds.cardinality();
				for (int i = 0; i < numOrds; ++i) {
					bitset.set(globalOrds.ordAt(i));
				}
			}
		}

		@Override
		public void postCollect() {

		}

		@Override
		public void close() throws ElasticsearchException {

		}
	}

	public static class Factory extends ValuesSourceAggregatorFactory<ValuesSource> {

		Factory(String name, ValuesSourceConfig config) {
			super(name, InternalDistinct.TYPE.name(), config);
		}

		@Override
		protected Aggregator createUnmapped(AggregationContext aggregationContext, Aggregator parent) {
			return new DistinctAggregator(name, parent == null ? 1 : parent.estimatedBucketCount(), null, config.formatter(), aggregationContext, parent);
		}

		@Override
		protected Aggregator create(ValuesSource valuesSource, long expectedBucketsCount, AggregationContext aggregationContext, Aggregator parent) {
			return new DistinctAggregator(name, parent == null ? 1 : parent.estimatedBucketCount(), valuesSource, config.formatter(), aggregationContext, parent);
		}
	}
}

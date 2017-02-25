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

import org.elasticsearch.common.inject.internal.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregationStreams;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.InternalNumericMetricsAggregation;
import org.elasticsearch.search.aggregations.support.format.ValueFormatter;
import org.elasticsearch.search.aggregations.support.format.ValueFormatterStreams;

import java.io.IOException;
import java.util.List;

/**
 * Author colin.ke keqinwu@yy.com
 */
public final class InternalDistinct extends InternalNumericMetricsAggregation.SingleValue implements Distinct {

	public final static Type TYPE = new Type("distinct");

	public final static AggregationStreams.Stream STREAM = new AggregationStreams.Stream() {
		@Override
		public InternalDistinct readResult(StreamInput in) throws IOException {
			InternalDistinct result = new InternalDistinct();
			result.readFrom(in);
			return result;
		}
	};

	public static void registerStreams() {
		AggregationStreams.registerStream(STREAM, TYPE.stream());
	}

	private long distinctCount;


	InternalDistinct(String name, @Nullable ValueFormatter formatter, long distinctCount) {
		super(name);
		this.valueFormatter = formatter;
		this.distinctCount = distinctCount;
	}


	private InternalDistinct() {
	}

	@Override
	public double value() {
		return getValue();
	}

	@Override
	public long getValue() {
		return distinctCount;
	}

	@Override
	public Type type() {
		return TYPE;
	}

	@Override
	public void readFrom(StreamInput in) throws IOException {
		name = in.readString();
		valueFormatter = ValueFormatterStreams.readOptional(in);
		distinctCount = in.readLong();
	}

	@Override
	public void writeTo(StreamOutput out) throws IOException {
		out.writeString(name);
		ValueFormatterStreams.writeOptional(valueFormatter, out);
		out.writeLong(distinctCount);
	}

	@Override
	public InternalAggregation reduce(ReduceContext reduceContext) {
		List<InternalAggregation> aggregations = reduceContext.aggregations();
		InternalDistinct reduced = null;
		for (InternalAggregation aggregation : aggregations) {
			final InternalDistinct cardinality = (InternalDistinct) aggregation;
			if (reduced == null) {
				reduced = new InternalDistinct(name, this.valueFormatter, 0);
			}
			reduced.distinctCount += cardinality.distinctCount;
		}

		return reduced;
	}

	@Override
	public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
		final long cardinality = getValue();
		builder.field(CommonFields.VALUE, cardinality);
		if (valueFormatter != null && !(valueFormatter instanceof ValueFormatter.Raw)) {
			builder.field(CommonFields.VALUE_AS_STRING, valueFormatter.format(cardinality));
		}
		return builder;
	}

}

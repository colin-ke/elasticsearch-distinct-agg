package com.yy.elasticsearch.distinct.aggregation;

import org.elasticsearch.search.aggregations.Aggregation;

/**
 * Author colin.ke keqinwu@yy.com
 */
public interface Distinct extends Aggregation {

	/**
	 * The number of unique terms.
	 */
	long getValue();
}

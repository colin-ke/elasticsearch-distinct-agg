package com.yy.elasticsearch.distinct;

import com.yy.elasticsearch.distinct.aggregation.DistinctParser;
import com.yy.elasticsearch.distinct.aggregation.InternalDistinct;
import org.elasticsearch.Version;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.search.aggregations.AggregationModule;

/**
 * Author colin.ke keqinwu@yy.com
 */
public class Plugin extends AbstractPlugin {

	private boolean enable = true;
	private final int MIN_SUPPORT_VERSION_ID = 1000001; //V_1_0_0_Beta1
	private final int MAX_SUPPORT_VERSION_ID = 2000001; //V_2_0_0_beta1

	public Plugin(){
		if(Version.CURRENT.id < MIN_SUPPORT_VERSION_ID || Version.CURRENT.id >= MAX_SUPPORT_VERSION_ID)
			enable = false;
	}

	@Override
	public String name() {
		return "distinct-agg";
	}

	@Override
	public String description() {
		return "aggregator for distinct";
	}

	@Override
	public void processModule(Module module) {
		if(module instanceof AggregationModule && enable){
			((AggregationModule) module).addAggregatorParser(DistinctParser.class);
			InternalDistinct.registerStreams();
		}
	}
}

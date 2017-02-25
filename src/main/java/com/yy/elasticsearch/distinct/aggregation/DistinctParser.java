package com.yy.elasticsearch.distinct.aggregation;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.metrics.cardinality.InternalCardinality;
import org.elasticsearch.search.aggregations.support.ValuesSourceParser;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

/**
 * Author colin.ke keqinwu@yy.com
 */
public class DistinctParser implements Aggregator.Parser {
	@Override
	public String type() {
		return InternalDistinct.TYPE.name();
	}

	@Override
	public AggregatorFactory parse(String name, XContentParser parser, SearchContext context) throws IOException {
		ValuesSourceParser vsParser = ValuesSourceParser.any(name, InternalDistinct.TYPE, context).formattable(false).build();

		XContentParser.Token token;
		String currentFieldName = null;
		while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
			if (token == XContentParser.Token.FIELD_NAME) {
				currentFieldName = parser.currentName();
			} else if (vsParser.token(currentFieldName, token, parser)) {
				continue;
			} else if (token.isValue()) {
					throw new SearchParseException(context, "Unknown key for a " + token + " in [" + name + "]: [" + currentFieldName + "].");
			} else {
				throw new SearchParseException(context, "Unexpected token " + token + " in [" + name + "].");
			}
		}


		return new DistinctAggregator.Factory(name, vsParser.config());
	}
}

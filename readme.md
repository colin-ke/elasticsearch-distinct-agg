# Elasticsearch Plugin -  Distinct Aggregation

With this es-plugin, it's possible for you to make a high performance accuracy distinct-query. But firstly, you need to index your data with routing_id set to the distinct field. As you can know that, we route the data according to the distinct field, so that, we only need to calculate distinct value in each shard, and sum them up to be the final result.  
currently supported elasticsearch version: 1.X

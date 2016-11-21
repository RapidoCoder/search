package org.xululabs.datasources;

import java.util.ArrayList;
import java.util.Map;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

public class ElasticsearchApi {
	/**
	 * use to get elasticsearch instance
	 * @param host
	 * @param port
	 * @return client
	 */
	public TransportClient getESInstance(String host, int port)throws Exception{
		 TransportClient client =  new TransportClient().addTransportAddress(new InetSocketTransportAddress(host, port));
		 return client;
	}
	
	public ArrayList<Map<String, Object>> searchDocuments(TransportClient client, String index, String fields[], String keyword, int documentsSize) throws Exception{
		
		ArrayList<Map<String, Object>> documents = new ArrayList<Map<String, Object>>();
		BoolQueryBuilder boolQuery = new BoolQueryBuilder();
		for (String field : fields){
		    boolQuery.should(QueryBuilders.matchPhraseQuery( field, keyword));
		}
		SearchResponse response = client.prepareSearch(index)
			    .setSearchType(SearchType.QUERY_THEN_FETCH)
			    .setQuery(boolQuery)
			    .setFrom(0).setSize(documentsSize).setExplain(true)
			    .execute()
			    .actionGet();
			SearchHit[] results = response.getHits().getHits();
			for (SearchHit hit : results) {
			  Map<String,Object> result = hit.getSource();   //the retrieved document
			  documents.add(result);
			}
		//close client
		client.close();
		return documents;
	}

}

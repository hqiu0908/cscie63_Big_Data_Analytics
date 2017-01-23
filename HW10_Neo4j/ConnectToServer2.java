package org.neo4j.graphproject;

import java.net.URISyntaxException;

import javax.ws.rs.core.MediaType;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

public class ConnectToServer2
{
	private static final String SERVER_ROOT_URI = "http://localhost:7474/db/data/";

	public static void main( String[] args ) throws URISyntaxException
	{
		checkDatabaseIsRunning();
		
		sendTransactionalCypherQuery( "MATCH (a:Actor { name:'Keanu Reeves' }) "
				+ "CREATE (johnwick:Movie { title : 'John Wick', year : '2014-10-24' }) "
				+ "CREATE (a)-[:ACTS_IN { role : 'John' }]->(johnwick)"
				+ "CREATE (chad:Director { name:'Chad Stahelski' }) "
				+ "CREATE (david:Director { name:'David Leitch' }) "
				+ "CREATE (william:Actor { name:'William Dafoe' }) "
				+ "CREATE (michael:Actor { name:'Michael Nyquist' }) "
				+ "CREATE (william)-[:ACTS_IN { role : 'Marcus' }]->(johnwick) "
				+ "CREATE (michael)-[:ACTS_IN { role : 'Viggo' }]->(johnwick) "
				+ "CREATE (chad)-[:DIRECTS]->(johnwick) "
				+ "CREATE (david)-[:DIRECTS]->(johnwick)" );
		
		sendTransactionalCypherQuery( "MATCH (a:Actor)-[:ACTS_IN]->(movie:Movie)<-[:ACTS_IN]-(keanu:Actor) "
				+ "RETURN DISTINCT a" );
		
		sendTransactionalCypherQuery( "MATCH (d:Director)-[:DIRECTS]->(movie:Movie)<-[:ACTS_IN]-(keanu:Actor) "
				+ "RETURN DISTINCT d" );	
		
		System.out.println("\nDelete the database and recreate by loading CSV files.\n");
		
		sendTransactionalCypherQuery ("MATCH (n) DETACH DELETE n");
		
		sendTransactionalCypherQuery ("START n=node(*) MATCH (n)-[r]->(m) RETURN n,r,m;");
		
		sendTransactionalCypherQuery ("LOAD CSV WITH HEADERS FROM 'file:///actors.csv' AS line "
				+ "CREATE (a:Actor { name:line.name });");
		
		sendTransactionalCypherQuery ("LOAD CSV WITH HEADERS FROM 'file:///movies.csv' AS line "
				+ "CREATE (m:Movie { title:line.title, year:line.year });");
		
		sendTransactionalCypherQuery ("LOAD CSV WITH HEADERS FROM 'file:///directors.csv' AS line "
				+ "CREATE (d:Director { name:line.name });");
		
		sendTransactionalCypherQuery ("LOAD CSV WITH HEADERS FROM 'file:///movie_actor_role.csv' AS line "
				+ "MATCH (a:Actor { name:line.name }) "
				+ "MATCH (m:Movie { title:line.title }) "
				+ "CREATE (a)-[:ACTS_IN { role: line.role }]->(m);");
		
		sendTransactionalCypherQuery ("LOAD CSV WITH HEADERS FROM 'file:///movie_director.csv' AS line "
				+ "MATCH (d:Director { name:line.name }) "
				+ "MATCH (m:Movie { title:line.title }) "
				+ "CREATE (d)-[:DIRECTS]->(m);");
		
		sendTransactionalCypherQuery ("START n=node(*) MATCH (n)-[r]->(m) RETURN n,r,m;");
	}

	private static void sendTransactionalCypherQuery(String query) {
		// START SNIPPET: queryAllNodes
		final String txUri = SERVER_ROOT_URI + "transaction/commit";
		WebResource resource = Client.create().resource( txUri );

		String payload = "{\"statements\" : [ {\"statement\" : \"" +query + "\"} ]}";
		ClientResponse response = resource
				.accept( MediaType.APPLICATION_JSON )
				.type( MediaType.APPLICATION_JSON )
				.entity( payload )
				.post( ClientResponse.class );

		System.out.println( String.format(
				"POST [%s] to [%s], status code [%d], returned data: "
						+ System.lineSeparator() + "%s",
						payload, txUri, response.getStatus(),
						response.getEntity( String.class ) ) );

		response.close();
		// END SNIPPET: queryAllNodes
	}

	private static void checkDatabaseIsRunning()
	{
		// START SNIPPET: checkServer
		WebResource resource = Client.create()
				.resource( SERVER_ROOT_URI );
		ClientResponse response = resource.get( ClientResponse.class );

		System.out.println( String.format( "GET on [%s], status code [%d]",
				SERVER_ROOT_URI, response.getStatus() ) );
		response.close();
		// END SNIPPET: checkServer
	}
}
package org.neo4j.graphproject;

import java.net.URI;
import java.net.URISyntaxException;

import javax.ws.rs.core.MediaType;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

public class ConnectToServer
{
	private static final String SERVER_ROOT_URI = "http://localhost:7474/db/data/";

	public static void main( String[] args ) throws URISyntaxException
	{
		checkDatabaseIsRunning();

		// START SNIPPET: nodes, props, labels
		URI firstNode = createNode();
		addProperty( firstNode, "title", "John Wick" );
		addProperty( firstNode, "year", "2014-10-24" );
		addLabel( firstNode, "Movie");

		URI secondNode = createNode();
		addProperty( secondNode, "name", "Chad Stahelski" );
		addLabel( secondNode, "Director");

		URI thirdNode = createNode();
		addProperty( thirdNode, "name", "David Leitch" );
		addLabel( thirdNode, "Director");

		URI fourthNode = createNode();
		addProperty( fourthNode, "name", "William Dafoe" );	
		addLabel( fourthNode, "Actor");

		URI fifthNode = createNode();
		addProperty( fifthNode, "name", "Michael Nyquist" );	
		addLabel( fifthNode, "Actor");
		// END SNIPPET: nodes, props, labels

		// START SNIPPET: addRel	
		addRelationship( secondNode, firstNode, "DIRECTS", "{ \"role\" : \"Director\"}" );

		addRelationship( thirdNode, firstNode, "DIRECTS", "{ \"role\" : \"Director\"}" );

		addRelationship( fourthNode, firstNode, "ACTS_IN", "{ \"role\" : \"Marcus\"}" );

		addRelationship( fifthNode, firstNode, "ACTS_IN", "{ \"role\" : \"Viggo\"}" );         
		// END SNIPPET: addRel

		sendTransactionalCypherQuery( "MATCH (a:Actor { name:\'Keanu Reeves\' }) "
				+ "MATCH (m:Movie { title:\'John Wick\' }) "
				+ "CREATE (a)-[:ACTS_IN { role : 'John' }]->(m)" );

		sendTransactionalCypherQuery( "MATCH (a:Actor)-[:ACTS_IN]->(movie:Movie)<-[:ACTS_IN]-(keanu:Actor) "
				+ "RETURN DISTINCT a" );

		sendTransactionalCypherQuery( "MATCH (d:Director)-[:DIRECTS]->(movie:Movie)<-[:ACTS_IN]-(keanu:Actor) "
				+ "RETURN DISTINCT d" );	
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

	private static void findSingersInBands( URI startNode )
			throws URISyntaxException
	{
		// START SNIPPET: traversalDesc
		// TraversalDefinition turns into JSON to send to the Server
		TraversalDefinition t = new TraversalDefinition();
		t.setOrder( TraversalDefinition.DEPTH_FIRST );
		t.setUniqueness( TraversalDefinition.NODE );
		t.setMaxDepth( 10 );
		t.setReturnFilter( TraversalDefinition.ALL );
		t.setRelationships( new Relation( "singer", Relation.OUT ) );
		// END SNIPPET: traversalDesc

		// START SNIPPET: traverse
		URI traverserUri = new URI( startNode.toString() + "/traverse/node" );
		WebResource resource = Client.create()
				.resource( traverserUri );
		String jsonTraverserPayload = t.toJson();
		ClientResponse response = resource.accept( MediaType.APPLICATION_JSON )
				.type( MediaType.APPLICATION_JSON )
				.entity( jsonTraverserPayload )
				.post( ClientResponse.class );

		System.out.println( String.format(
				"POST [%s] to [%s], status code [%d], returned data: "
						+ System.lineSeparator() + "%s",
						jsonTraverserPayload, traverserUri, response.getStatus(),
						response.getEntity( String.class ) ) );
		response.close();
		// END SNIPPET: traverse
	}

	// START SNIPPET: insideAddMetaToProp
	private static void addMetadataToProperty( URI relationshipUri,
			String name, String value ) throws URISyntaxException
	{
		URI propertyUri = new URI( relationshipUri.toString() + "/properties" );
		String entity = toJsonNameValuePairCollection( name, value );
		WebResource resource = Client.create()
				.resource( propertyUri );
		ClientResponse response = resource.accept( MediaType.APPLICATION_JSON )
				.type( MediaType.APPLICATION_JSON )
				.entity( entity )
				.put( ClientResponse.class );

		System.out.println( String.format(
				"PUT [%s] to [%s], status code [%d]", entity, propertyUri,
				response.getStatus() ) );
		response.close();
	}

	// END SNIPPET: insideAddMetaToProp

	private static String toJsonNameValuePairCollection( String name,
			String value )
	{
		return String.format( "{ \"%s\" : \"%s\" }", name, value );
	}

	private static URI createNode()
	{
		// START SNIPPET: createNode
		// final String txUri = SERVER_ROOT_URI + "transaction/commit";
		final String nodeEntryPointUri = SERVER_ROOT_URI + "node";
		// http://localhost:7474/db/data/node

		WebResource resource = Client.create()
				.resource( nodeEntryPointUri );
		// POST {} to the node entry point URI
		ClientResponse response = resource.accept( MediaType.APPLICATION_JSON )
				.type( MediaType.APPLICATION_JSON )
				.entity( "{}" )
				.post( ClientResponse.class );

		final URI location = response.getLocation();
		System.out.println( String.format(
				"POST to [%s], status code [%d], location header [%s]",
				nodeEntryPointUri, response.getStatus(), location.toString()));
		response.close();

		return location;
		// END SNIPPET: createNode
	}

	// START SNIPPET: insideAddRel
	private static URI addRelationship( URI startNode, URI endNode,
			String relationshipType, String jsonAttributes )
					throws URISyntaxException
	{
		URI fromUri = new URI( startNode.toString() + "/relationships" );
		String relationshipJson = generateJsonRelationship( endNode,
				relationshipType, jsonAttributes );

		WebResource resource = Client.create()
				.resource( fromUri );
		// POST JSON to the relationships URI
		ClientResponse response = resource.accept( MediaType.APPLICATION_JSON )
				.type( MediaType.APPLICATION_JSON )
				.entity( relationshipJson )
				.post( ClientResponse.class );

		final URI location = response.getLocation();
		System.out.println( String.format(
				"POST to [%s], status code [%d], location header [%s]",
				fromUri, response.getStatus(), location.toString() ) );

		response.close();
		return location;
	}
	// END SNIPPET: insideAddRel

	private static String generateJsonRelationship( URI endNode,
			String relationshipType, String... jsonAttributes )
	{
		StringBuilder sb = new StringBuilder();
		sb.append( "{ \"to\" : \"" );
		sb.append( endNode.toString() );
		sb.append( "\", " );

		sb.append( "\"type\" : \"" );
		sb.append( relationshipType );
		if ( jsonAttributes == null || jsonAttributes.length < 1 )
		{
			sb.append( "\"" );
		}
		else
		{
			sb.append( "\", \"data\" : " );
			for ( int i = 0; i < jsonAttributes.length; i++ )
			{
				sb.append( jsonAttributes[i] );
				if ( i < jsonAttributes.length - 1 )
				{ // Miss off the final comma
					sb.append( ", " );
				}
			}
		}

		sb.append( " }" );
		return sb.toString();
	}

	private static void addLabel( URI nodeUri, String labelValue )
	{
		// START SNIPPET: addLabel
		String propertyUri = nodeUri.toString() + "/labels";
		// http://localhost:7474/db/data/node/{node_id}/labels

		WebResource resource = Client.create()
				.resource( propertyUri );
		ClientResponse response = resource.accept( MediaType.APPLICATION_JSON )
				.type( MediaType.APPLICATION_JSON )
				.entity( "\"" + labelValue + "\"" )
				.post( ClientResponse.class );

		System.out.println( String.format( "PUT to [%s], status code [%d]",
				propertyUri, response.getStatus() ) );
		response.close();
		// END SNIPPET: addLabel
	}	

	private static void addProperty( URI nodeUri, String propertyName,
			String propertyValue )
	{
		// START SNIPPET: addProp
		String propertyUri = nodeUri.toString() + "/properties/" + propertyName;
		// http://localhost:7474/db/data/node/{node_id}/properties/{property_name}

		WebResource resource = Client.create()
				.resource( propertyUri );
		ClientResponse response = resource.accept( MediaType.APPLICATION_JSON )
				.type( MediaType.APPLICATION_JSON )
				.entity( "\"" + propertyValue + "\"" )
				.put( ClientResponse.class );

		System.out.println( String.format( "PUT to [%s], status code [%d]",
				propertyUri, response.getStatus() ) );
		response.close();
		// END SNIPPET: addProp
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
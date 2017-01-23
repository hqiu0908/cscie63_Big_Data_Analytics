package edu.hu.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.BoundStatement;

import java.util.UUID;

public class PreparedClient {
	private Cluster cluster;
	private Session session;

	public Session getSession() {
		return this.session;
	}
	
	public void connect(String node) {
		cluster = Cluster.builder()
				.addContactPoint(node).build();
		session = cluster.connect();
		Metadata metadata = cluster.getMetadata();
		System.out.printf("Connected to cluster: %s\n", 
				metadata.getClusterName());
		for ( Host host : metadata.getAllHosts() ) {
			System.out.printf("Datatacenter: %s; Host: %s; Rack: %s\n",
					host.getDatacenter(), host.getAddress(), host.getRack());
		}
	}

	public void createSchema() {
		session.execute("CREATE KEYSPACE newkeyspace WITH replication " + 
				"= {'class':'SimpleStrategy', 'replication_factor':1};");

		session.execute(
				"CREATE TABLE newkeyspace.person (" +
						"id uuid PRIMARY KEY," + 
						"fname text," + 
						"lname text," + 
						"city text," + 
						"phone1 text," + 
						"phone2 text," +
						"phone3 text" +
				");");
	}

	public void loadData() {
		PreparedStatement statement = getSession().prepare(
      		"INSERT INTO newkeyspace.person " +
      		"(id, fname, lname, city, phone1, phone2, phone3) " +
      		"VALUES (?, ?, ?, ?, ?, ?);");
      		
      	BoundStatement boundStatement = new BoundStatement(statement);

		getSession().execute(boundStatement.bind(
      		UUID.fromString("de305d54-75b4-431b-adb2-eb6b9e546001"),
      		"Nick",
      		"Smith",
      		"Boston",
      		"(781)397-7171",
      		"(781)393-5303",
      		"(781)286-8925") );

		getSession().execute(boundStatement.bind(
      		UUID.fromString("de305d54-75b4-431b-adb2-eb6b9e546002"),
      		"Joe",
      		"White",
      		"Cambridge",
      		"(617)321-1805",
      		"(617)346-1273",
      		"(617)319-1835") );

		getSession().execute(boundStatement.bind(
      		UUID.fromString("de305d54-75b4-431b-adb2-eb6b9e546003"),
      		"Mary",
      		"Earl",
      		"Newton",
      		"(854)317-3715",
      		"(854)652-2482",
      		"(854)592-1753") );
	}

	public void querySchema(){
		ResultSet results = session.execute("SELECT * FROM newkeyspace.person");
		System.out.println(String.format("%-5s\t%-10s\t%-10s\t%-10s\t%-15s\t%-15s\t%-15s\n%s", 
				"id", "lname", "fname", "city", "phone1", "phone2", "phone3", 
				"-------+---------------+---------------+---------------+---------------+---------------+---------------"));
		for (Row row : results) {
			System.out.println(String.format("%-5s\t%-10s\t%-10s\t%-10s\t%-15s\t%-15s\t%-15s", row.getInt("id"),
					row.getString("lname"), row.getString("fname"), row.getString("city"),
					row.getString("phone1"), row.getString("phone2"), row.getString("phone3")));
		}
		System.out.println();


	}

	public void close() {
		cluster.close(); // .shutdown();
	}

	public static void main(String[] args) {
		CQLClient client = new CQLClient();
		client.connect("127.0.0.1");
		client.createSchema();
		client.loadData();
		client.querySchema();
		client.close();
	}
}
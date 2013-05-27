package com.apptsunami.samples.yelpdatachallenge;

import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.WriteConcern;

/**
 * Super class to support database operation classes
 * 
 * @author stevec
 *
 */
public class DatabaseOperation {
	
	private static final String DB_HOST = "localhost";
	private static final int DB_PORT = 27017;
	private static final String DB_NAME = "yelpdatachallenge";
	private static final String DB_USER = null;
	private static final String DB_PASSWORD = null;
	
	protected MongoClient mongoClient = null;
	protected DB db = null;
	
	/* constructors */
	public DatabaseOperation() {
		/* no-op */
	} // DatabaseOperation
	
	public void openDatabase() throws Exception {
		 mongoClient = new MongoClient(DB_HOST, DB_PORT);
		 db = mongoClient.getDB(DB_NAME);
		if ((DB_USER != null) && (DB_PASSWORD != null)) {
			boolean auth = db.authenticate(DB_USER, DB_PASSWORD.toCharArray());
			if (!auth) {
				throw new Exception("Incorrect user name or password");
			}
		} // if
		mongoClient.setWriteConcern(WriteConcern.JOURNALED);
	} // openDatabase
	
	public void closeDatabase() {
		if (mongoClient != null)
		mongoClient.close();
	} // closeDatabase

} // DatabaseOperation

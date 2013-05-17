package com.apptsunami.samples.yelpdatachallenge.parse;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.WriteConcern;
import com.mongodb.util.JSON;

public class ProcessJsonFile {

	private static final String DB_HOST = "localhost";
	private static final int DB_PORT = 27017;
	private static final String DB_NAME = "yelpdatachallenge";
	private static final String DB_USER = null;
	private static final String DB_PASSWORD = null;

	/* constructors */
	public ProcessJsonFile() {
		/* no-op */
	} // JsonFileParser

	/**
	 * 
	 * @param db
	 * @param collectionName
	 * @return number of records found in the collection
	 * 
	 * This function is for debugging purposes.  It prints every record in a
	 *   collection and returns the number of records found.
	 */
	private int dumpCollection(DB db, String collectionName) {
		DBCollection coll = db.getCollection(collectionName);
		if (coll == null) {
			System.out.println("Cannot get collection " + collectionName);
			return 0;
		}
		System.out.println("Dump collection " + collectionName);
		System.out.println("====");
		DBCursor cursorDoc = coll.find();
		int count = 0;
		while (cursorDoc.hasNext()) {
			System.out.println(cursorDoc.next());
			count++;
		} // while
		cursorDoc.close();
		System.out.println("====");
		return count;
	} // dumpCollection

	/**
	 * 
	 * @param inputFile
	 * @param collectionName
	 * @throws Exception
	 * 
	 * Reads a file where each line is a json object.  The json will be
	 * loaded as a record into the collection.  It then dumps all records
	 * in the collection and verifies the number of records equals the
	 * number of lines read in the file.
	 * 
	 */
	public void processFile(File inputFile, String collectionName)
			throws Exception {
		MongoClient mongoClient = new MongoClient(DB_HOST, DB_PORT);
		DB db = mongoClient.getDB(DB_NAME);
		if ((DB_USER != null) && (DB_PASSWORD != null)) {
			boolean auth = db.authenticate(DB_USER, DB_PASSWORD.toCharArray());
			if (!auth) {
				throw new Exception("Incorrect user name or password");
			}
		} // if
		mongoClient.setWriteConcern(WriteConcern.JOURNALED);

		DBCollection coll = db.getCollection(collectionName);
		coll.drop();
		BufferedReader br = new BufferedReader(new FileReader(inputFile));
		String line = null;
		int lineCount = 0;
		while ((line = br.readLine()) != null) {
			DBObject dbObject = (DBObject) JSON.parse(line);
			coll.insert(dbObject);
			lineCount++;
		} // while
		br.close();

		/* verify */
		int dumpCount = dumpCollection(db, collectionName);
		if (dumpCount != lineCount) {
			System.out.println("Read " + lineCount
					+ " from data file but retrieved " + dumpCount
					+ " objects in collection " + collectionName);
		}
		/* done */
		mongoClient.close();
	} // processFile

} // JsonFileParser

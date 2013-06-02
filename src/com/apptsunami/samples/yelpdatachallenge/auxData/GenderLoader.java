package com.apptsunami.samples.yelpdatachallenge.auxData;

import com.apptsunami.samples.yelpdatachallenge.DatabaseOperation;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class GenderLoader extends DatabaseOperation {

	private void ensureCollectionUserIndex(DBCollection coll) {
		coll.ensureIndex(CollectionUser.KEY_USER_ID);
	} // ensureCollectionUserIndex

	private void updateGenderFrequency(DBCollection userCollection,
			String userId, String key, double value) {
		/* update where user_id */
		final BasicDBObject query = new BasicDBObject(
				CollectionUser.KEY_USER_ID, userId);
		/* one new field */
		DBObject documentBuilder = BasicDBObjectBuilder.start().add(key, value)
				.get();
		/* upsert, not multi */
		userCollection.update(query,
				new BasicDBObject(UPSERT_SET, documentBuilder), true, false);
	} // updateGenderFrequency

	private void updateGenderFrequency(DBCollection userCollection,
			String userId, double maleFreq, double femaleFreq) {
		updateGenderFrequency(userCollection, userId,
				CollectionUser.KEY_MALE_PROBABILITY, maleFreq);
		updateGenderFrequency(userCollection, userId,
				CollectionUser.KEY_FEMALE_PROBABILITY, femaleFreq);
	} // updateGenderFrequency

	public void addGenderToUser() {
		DBCollection userCollection = db.getCollection(COLLECTION_USER);
		ensureCollectionUserIndex(userCollection);
		DBCursor cursorUser = userCollection.find();
		while (cursorUser.hasNext()) {
			DBObject userObject = cursorUser.next();
			String userId = (String) userObject.get(CollectionUser.KEY_USER_ID);
			String name = (String) userObject.get(CollectionUser.KEY_NAME);
			if (name != null) {
				/* same first name can be either gender */
				Double maleFrequency = FirstNameMale.get(name);
				Double femaleFrequency = FirstNameFemale.get(name);
				/* compute relative frequency */
				if (maleFrequency == null) {
					if (femaleFrequency == null) {
						/* no info */
					} else {
						/* 100% female */
						updateGenderFrequency(userCollection, userId, 0.0, 1.0);
					} // else
				} else {
					if (femaleFrequency == null) {
						/* 100% male */
						updateGenderFrequency(userCollection, userId, 1.0, 0.0);
					} else {
						/* assume male population is same as female population */
						double totalFreq = maleFrequency + femaleFrequency;
						double relativeMaleFreq = maleFrequency / totalFreq;
						double relativeFemaleFreq = femaleFrequency / totalFreq;
						updateGenderFrequency(userCollection, userId,
								relativeMaleFreq, relativeFemaleFreq);
					} // else
				} // else
			} // if
		} // while
	} // addGenderToUser

} // AddGender

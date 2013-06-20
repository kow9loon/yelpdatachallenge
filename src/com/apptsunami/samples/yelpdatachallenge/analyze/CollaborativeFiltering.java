package com.apptsunami.samples.yelpdatachallenge.analyze;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

import com.apptsunami.samples.yelpdatachallenge.DatabaseOperation;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

/**
 * This class supports the collaborative filtering algorithm.
 * 
 * @author stevec
 * 
 */
public class CollaborativeFiltering extends DatabaseOperation {

	/*
	 * All stars rating are between 1 and 5
	 */
	private static final int MINIMUM_STARS = 1;
	private static final int MAXIMUM_STARS = 5;

	/*
	 * Two users must have reviewed at least MINIMUM_COMMON_REVIEW_COUNT
	 * businesses in common for these users to be checked for similarity.
	 * Otherwise the pcc cannot be evaluated (the variance will be zero).
	 */
	private static final int MINIMUM_COMMON_REVIEW_COUNT = 2;
	/*
	 * Only this many similar users will be used to compute the predicted
	 * rating.
	 */
	private static final Integer MAXIMUM_RATING_SAMPLE = null;
	/*
	 * If negative pcc should be rejected
	 */
	private static final boolean REJECT_NEGATIVE_PCC = true;
	/*
	 * If pcc is below this threshold the similarity is too weak and considered
	 * noise
	 */
	private static final double MINIMUM_PCC_THRESHOLD = 0.4;
	/*
	 * The weight placed on the number of common businesses rated between two
	 * users
	 */
	private static final Double SAMPLE_COUNT_WEIGHT = 1.0;
	private static final Double SAME_GENDER_WEIGHT = 0.2;
	/*
	 * No need to cache after applying collection index. Otherwise we need to
	 * increase the jvm run time memory.
	 */
	private static final boolean CACHE_REVIEW_IN_MEMORY = false;
	/*
	 * A cache to look up all reviews by a user id
	 */
	private HashMap<String, HashMap<String, DBObject>> userReviewHash = new HashMap<String, HashMap<String, DBObject>>();

	/**
	 * Given a user and a business id stores the stars predicted by
	 * collaborative filtering
	 */
	private static class RatingResult {
		public String userId = null;
		public String businessId = null;
		public int stars = 0; // actual
		public int totalUserCount = 0; // total number of similar users
		public int actualUserCount = 0; // actual no. used to calculate
		// prediction
		public Double predictedStars = null; // prediction
		public Double averageRatingStars = null;

		public String toString() {
			return "{\"userId\":\'"
					+ userId
					+ "\", \"businessId\":\""
					+ businessId
					+ "\", \"stars\":\""
					+ stars
					+ "\", \"totalUserCount\":\""
					+ totalUserCount
					+ "\", \"actualUserCount\":\""
					+ actualUserCount
					+ "\""
					+ (predictedStars == null ? "" : ", \"predictedStars\":\""
							+ predictedStars + "\"") + "}";
		} // toString

	} // RatingResult

	/**
	 * Stores the similarity between two users.
	 * 
	 */
	private static class SimilarityCoefficient {
		public double pcc = 0.0; // pearson correlation coefficient
		public int count = 0; // number of data points used to calculate pcc
		public Double sameGenderProbability = null;
		public double meanX = 0.0;
		public double meanY = 0.0;
		public boolean xIsConstant = false;
		public boolean yIsConstant = false;

		public String toString() {
			return "{\"pcc\":\""
					+ pcc
					+ "\", \"count\":\""
					+ count
					+ "\", "
					+ (sameGenderProbability == null ? ""
							: "\"sameGenderProbability\":\""
									+ sameGenderProbability + "\", ")
					+ "\"meanX\":\"" + meanX + "\", \"xIsConstant\":\""
					+ xIsConstant + "\", \"meanY\":\"" + meanY
					+ "\", \"yIsConstant\":\"" + yIsConstant + "\"}";
		} // toString

	} // SimilarityCoefficient

	/**
	 * Comparator of SimilarityCoefficient
	 * 
	 */
	private static class SimilarityCoefficientComparator implements
			Comparator<SimilarityCoefficient> {
		@Override
		public int compare(SimilarityCoefficient o1, SimilarityCoefficient o2) {
			/* order by count */
			if (o1.count > o2.count) {
				return 1;
			} else if (o1.count < o2.count) {
				return -1;
			} else {
				/* then order by pcc */
				if (o1.pcc > o2.pcc) {
					return 1;
				} else if (o1.pcc < o2.pcc) {
					return -1;
				} else {
					return 0;
				}
			} // else
		} // compare
	} // SimilarityCoefficientComparator

	/**
	 * Stores the rating and similarity with another user. Note that the
	 * business id is not stored here because all objects in the ArrayList are
	 * for the same business
	 * 
	 */
	private static class ComparableRating {
		public String originalUserId = null; // for debug
		public String userId = null; // for debug
		public int stars = -1;
		public SimilarityCoefficient coefficient = null;

		public String toString() {
			return "{\"originalUserId\":\"" + originalUserId
					+ "\", \"userId\":\"" + userId + "\", \"stars\":\"" + stars
					+ "\", \"coefficient\":\"" + coefficient + "\"}";
		} // toString

	} // ComparableRating

	/**
	 * Comparator of ComparableRating. It compares the SimilarityCoefficient of
	 * the entries.
	 */
	private static class ComparableRatingComparator implements
			Comparator<ComparableRating> {
		private SimilarityCoefficientComparator scComparator = new SimilarityCoefficientComparator();

		@Override
		public int compare(ComparableRating o1, ComparableRating o2) {
			return scComparator.compare(o1.coefficient, o2.coefficient);
		}
	} // ComparableRatingComparator

	/**
	 * Utility function to fetch a review from the HashMap and then returns the
	 * stars attribute of the review
	 * 
	 * @param reviewArray
	 * @param key
	 * @return Stars attribute of a review object
	 */
	private Integer getReviewStarFromHashMap(
			HashMap<String, DBObject> reviewArray, String key) {
		DBObject dbo = reviewArray.get(key);
		if (dbo == null) {
			return null;
		} else {
			return (Integer) dbo.get(CollectionReview.KEY_STARS);
		}
	} // getReviewStar

	/**
	 * Computes the similarity of two users based on the reviews of the users.
	 * See http://en.wikipedia.org/wiki/Pearson_correlation_coefficient
	 * 
	 * @param reviewArray
	 *            All reviews of user 1 (business_id => review)
	 * @param reviewArray2
	 *            All reviews of user 2 (business_id => review)
	 * @param exceptBusinessId
	 *            Do not use the reviews of this business
	 * @return SimilarityCoefficient of the two users
	 */
	private SimilarityCoefficient computeSimilarityCoefficient(
			HashMap<String, DBObject> reviewArray,
			HashMap<String, DBObject> reviewArray2, String exceptBusinessId) {
		Set<String> keySet = reviewArray.keySet();
		Iterator<String> itr = keySet.iterator();
		int count = 0;
		int totalX = 0;
		int totalY = 0;
		ArrayList<Integer> xArr = new ArrayList<Integer>();
		ArrayList<Integer> yArr = new ArrayList<Integer>();
		while (itr.hasNext()) {
			String key = itr.next();
			/* ignore the reviews of the business id */
			if (key.equals(exceptBusinessId))
				continue;
			if (reviewArray2.containsKey(key)) {
				/* found reviews of the same business in both user 1 and user 2 */
				count++;
				/* get stars of user 1 */
				int x = getReviewStarFromHashMap(reviewArray, key);
				/* get stars of user 2 */
				int y = getReviewStarFromHashMap(reviewArray2, key);
				totalX += x;
				totalY += y;
				xArr.add(x);
				yArr.add(y);
			} // if
		}
		if (count < MINIMUM_COMMON_REVIEW_COUNT) {
			/*
			 * There needs to be at least 2 for pcc to be computable. We may
			 * increase this minimum to a higher number for better accuracy
			 */
			return null;
		} else {
			Double averageX = Double.valueOf(totalX) / count;
			Double averageY = Double.valueOf(totalY) / count;
			/* covariance is the numerator */
			Double covariance = 0.0;
			/* variance are the denominator */
			Double varianceX = 0.0;
			Double varianceY = 0.0;
			for (int i = 0; i < count; i++) {
				int xi = xArr.get(i);
				int yi = yArr.get(i);
				Double deltaX = xi - averageX;
				Double deltaY = yi - averageY;
				covariance += deltaX * deltaY;
				varianceX += deltaX * deltaX;
				varianceY += deltaY * deltaY;
				/*
				 * System.out.println("x=" + xi + " y=" + yi + " deltaX=" +
				 * deltaX + " deltaY=" + deltaY);
				 */
			} // for
			/*
			 * System.out.println(xArr); System.out.println(yArr);
			 * System.out.println("covariance=" + covariance + " averageX=" +
			 * averageX + " averageY=" + averageY + " varianceX=" + varianceX +
			 * " varianceY=" + varianceY);
			 */

			/*
			 * both the number of data points and the pcc are returned so they
			 * can considered in user selection
			 */
			SimilarityCoefficient coefficient = new SimilarityCoefficient();
			coefficient.count = count;
			coefficient.meanX = averageX;
			coefficient.meanY = averageY;
			coefficient.xIsConstant = (varianceX == 0);
			coefficient.yIsConstant = (varianceY == 0);
			if (coefficient.xIsConstant || coefficient.yIsConstant) {
				/*
				 * if the x or the y all have the same value then variance can
				 * be zero
				 */
				if (covariance > 0) {
					coefficient.pcc = 1.0;
				} else if (covariance < 0) {
					coefficient.pcc = -1.0;
				} else {
					coefficient.pcc = 0.0;
				}
			} else {
				coefficient.pcc = covariance
						/ (Math.sqrt(varianceX) * Math.sqrt(varianceY));
				/*
				 * System.out.println("pcc=" + coefficient.pcc);
				 */
			}
			return coefficient;
		} // else
	} // computeSimilarityCoefficient

	/**
	 * Returns all reviews by a user
	 * 
	 * @param reviewObject
	 * @return A HashMap of reviews where the key is the business id
	 */
	private HashMap<String, DBObject> findUserReviews(DBObject reviewObject) {
		String userId = (String) reviewObject.get(CollectionReview.KEY_USER_ID);
		HashMap<String, DBObject> reviewArray = null;
		if (CACHE_REVIEW_IN_MEMORY) {
			reviewArray = userReviewHash.get(userId);
			if (reviewArray != null) {
				return reviewArray;
			} // if
		} // if
		reviewArray = new HashMap<String, DBObject>();
		/* search for all reviews by the user id */
		DBCollection coll = db.getCollection(COLLECTION_REVIEW);
		BasicDBObject filter = new BasicDBObject();
		filter.put(CollectionReview.KEY_USER_ID, userId);
		DBCursor cursorReview3 = coll.find(filter);
		while (cursorReview3.hasNext()) {
			DBObject reviewObject3 = cursorReview3.next();
			/* insert the review with business id as the key */
			reviewArray.put((String) reviewObject3
					.get(CollectionReview.KEY_BUSINESS_ID), reviewObject3);
		} // while
		if (CACHE_REVIEW_IN_MEMORY) {
			userReviewHash.put(userId, reviewArray);
		}
		return reviewArray;
	} // findUserReviews

	/**
	 * Returns the number of other users' reviews to be used in the prediction
	 * calculation. It is the minimum of a hard-coded constant
	 * (MAXIMUM_RATING_SAMPLE) or the number of other users' reviews.
	 * 
	 * @param comparableRatingArray
	 * @return
	 */
	private int calculateLimitOnRating(
			ArrayList<ComparableRating> comparableRatingArray) {
		if (comparableRatingArray.isEmpty())
			return 0;
		int count = comparableRatingArray.size();
		if (MAXIMUM_RATING_SAMPLE == null) {
			return count;
		} else {
			return Math.min(count, MAXIMUM_RATING_SAMPLE);
		}
	} // calculateLimitOnRating

	/**
	 * Sort the other user's reviews so the 'more similar' reviews are at the
	 * top
	 * 
	 * @param comparableRatingArray
	 * @return The maximum number of reviews to use
	 */
	private int limitComparableRating(
			ArrayList<ComparableRating> comparableRatingArray) {
		if (comparableRatingArray == null)
			return 0;
		Collections.sort(comparableRatingArray,
				new ComparableRatingComparator());
		return calculateLimitOnRating(comparableRatingArray);
	} // limitComparableRating

	/**
	 * Use other users' stars to predict the (original) user's stars. It uses
	 * the basic weighted formula where the weight is the similarity with the
	 * (original) user.
	 * 
	 * @param comparableRatingArray
	 *            An array of other users' reviews
	 * @param limit
	 *            The maximum number of reviews to be used
	 * @return The predicted stars of the (original) user
	 */
	private Double computeComparableRating(
			ArrayList<ComparableRating> comparableRatingArray, int limit) {
		if (comparableRatingArray == null)
			return null;
		Iterator<ComparableRating> itr = comparableRatingArray.iterator();
		Double totalStars = 0.0;
		Double totalCoefficient = 0.0;
		int count = 0;
		while (itr.hasNext()) {
			if (count > limit)
				break;
			ComparableRating comparableRating = itr.next();
			Double significance = Math.abs(comparableRating.coefficient.pcc);
			if (comparableRating.coefficient.xIsConstant) {
				if (comparableRating.coefficient.yIsConstant) {
					/*
					 * x - y is a constant so just add this as offset to the y
					 * rating
					 */
					double stars = comparableRating.stars
							- comparableRating.coefficient.meanY
							+ comparableRating.coefficient.meanX;
					totalStars += stars * significance;
					count++;
				} else {
					/*
					 * x is a constant but y is not so just take the x rating as
					 * the prediction
					 */
					totalStars += comparableRating.coefficient.meanX
							* significance;
					count++;
				} // else
			} else if (comparableRating.coefficient.pcc != 0.0) {
				/* pcc of zero means no prediction value */
				if ((SAMPLE_COUNT_WEIGHT != null)
						&& (SAMPLE_COUNT_WEIGHT != 0.0)) {
					double factor = 1 - (1 / (comparableRating.coefficient.count * SAMPLE_COUNT_WEIGHT));
					significance *= factor;
				} // if
				if (comparableRating.coefficient.sameGenderProbability != null) {
					double genderFactor = comparableRating.coefficient.sameGenderProbability
							* SAME_GENDER_WEIGHT;
					significance += genderFactor;
				} // if
				totalCoefficient += significance;
				int stars;
				if (comparableRating.coefficient.pcc > 0) {
					stars = comparableRating.stars;
				} else {
					/* invert the scale if the pcc is negative */
					stars = MAXIMUM_STARS - comparableRating.stars
							+ MINIMUM_STARS;
				} // else if
				totalStars += stars * significance;
				count++;
			} // else
		} // while
		if (count == 0) {
			return null;
		} else {
			/* normalize by the total weight */
			return totalStars / totalCoefficient;
		}
	} // computeComparableRating

	/**
	 * A coefficient is not usable if: 1) pcc is zero 2) pcc is negative 3) pcc
	 * is less than the minimum threshold
	 * 
	 * We may add other criteria, e.g. the number of samples used to compute the
	 * pcc
	 * 
	 * @param coefficient
	 * @return
	 */
	private boolean isCoefficientUsable(SimilarityCoefficient coefficient) {
		if (coefficient == null)
			return false;
		if (coefficient.pcc == 0.0)
			return false;
		if (REJECT_NEGATIVE_PCC && (coefficient.pcc < 0))
			return false;
		if (coefficient.pcc < MINIMUM_PCC_THRESHOLD)
			return false;
		return true;
	} // isCoefficientUsable

	private DBObject getUserByUserId(DBCollection userCollection, String userId) {
		BasicDBObject filter = new BasicDBObject();
		filter.put(CollectionUser.KEY_USER_ID, userId);
		return userCollection.findOne(filter);
	} // getUserByUserId

	private Double calculateSameGenderProbability(DBCollection userCollection,
			String userId1, String userId2) {
		DBObject user1 = getUserByUserId(userCollection, userId1);
		DBObject user2 = getUserByUserId(userCollection, userId2);
		if ((user1 == null) || (user2 == null)) {
			return null;
		}
		String name1 = (String) user1.get(CollectionUser.KEY_NAME);
		if (name1 != null) {
			String name2 = (String) user2.get(CollectionUser.KEY_NAME);
			if (name1.equals(name2))
				return 1.0;
		} // if
		Double fp1 = (Double) user1.get(CollectionUser.KEY_FEMALE_PROBABILITY);
		Double mp1 = (Double) user1.get(CollectionUser.KEY_MALE_PROBABILITY);
		Double fp2 = (Double) user2.get(CollectionUser.KEY_FEMALE_PROBABILITY);
		Double mp2 = (Double) user2.get(CollectionUser.KEY_MALE_PROBABILITY);
		Double p = null;
		if ((fp1 != null) && (fp2 != null)) {
			if (p == null) {
				p = fp1 * fp2;
			} else {
				p += fp1 * fp2;
			}
		} // if
		if ((mp1 != null) && (mp2 != null)) {
			if (p == null) {
				p = mp1 * mp2;
			} else {
				p += mp1 * mp2;
			}
		} // if
		return p;
	} // calculateSameGenderProbability

	/**
	 * Main collaborative filtering function
	 * 
	 * @param reviewObject
	 *            The review object. We apply the collaborative filtering
	 *            algorithm and compares the predicted stars with the actual
	 *            stars of the review.
	 * @return RatingResult An object containing the actual stars and the
	 *         predicted stars
	 */
	private RatingResult predictUserRating(DBObject reviewObject) {
		if (reviewObject == null)
			return null;
		if (!reviewObject.containsField(CollectionReview.KEY_USER_ID)
				|| !reviewObject
						.containsField(CollectionReview.KEY_BUSINESS_ID)
				|| !reviewObject.containsField(CollectionReview.KEY_STARS)) {
			return null;
		}
		RatingResult ratingResult = new RatingResult();
		/* user id of the (original) user */
		ratingResult.userId = (String) reviewObject
				.get(CollectionReview.KEY_USER_ID);
		/* business id of the (original) business */
		ratingResult.businessId = (String) reviewObject
				.get(CollectionReview.KEY_BUSINESS_ID);
		/*
		 * actual stars rating of the (original) review. The rest of the logic
		 * tries to use collaborative filtering to predict this values and then
		 * compares the prediction vs. the actual.
		 */
		ratingResult.stars = (Integer) reviewObject
				.get(CollectionReview.KEY_STARS);
		/* collect all reviews of the (original) user */
		HashMap<String, DBObject> reviewArray = findUserReviews(reviewObject);
		System.out.println("Try to predict user " + ratingResult.userId
				+ " for business " + ratingResult.businessId);
		/* search for all reviews for the same business */
		DBCollection reviewCollection = db.getCollection(COLLECTION_REVIEW);
		BasicDBObject filter = new BasicDBObject();
		filter.put(CollectionReview.KEY_BUSINESS_ID, ratingResult.businessId);
		DBCursor cursorReview2 = reviewCollection.find(filter);
		/*
		 * comparableRatingArray contains the reviews by other users of the
		 * (original) business
		 */
		ArrayList<ComparableRating> comparableRatingArray = new ArrayList<ComparableRating>();
		DBCollection userCollection = db.getCollection(COLLECTION_USER);
		ensureCollectionUserIndex(userCollection);
		int totalRatingCount = 0;
		int totalRatingStars = 0;
		/* for each review of the (original) business */
		while (cursorReview2.hasNext()) {
			DBObject reviewObject2 = cursorReview2.next();
			String userId2 = (String) reviewObject2
					.get(CollectionReview.KEY_USER_ID);
			/* make sure to skip the review by the (original) business */
			if (!userId2.equals(ratingResult.userId)) {
				totalRatingCount++;
				totalRatingStars += (Integer) reviewObject2
						.get(CollectionReview.KEY_STARS);
				/* collect all reviews of this other user */
				HashMap<String, DBObject> reviewArray2 = findUserReviews(reviewObject2);
				/*
				 * compute similarity between this other user and the (original)
				 * user
				 */
				SimilarityCoefficient coefficient = computeSimilarityCoefficient(
						reviewArray, reviewArray2, ratingResult.businessId);
				/* sometimes similarity cannot be computed so we need to check */
				if (isCoefficientUsable(coefficient)) {
					coefficient.sameGenderProbability = calculateSameGenderProbability(
							userCollection, ratingResult.userId, userId2);
					DBObject businessReview = reviewArray2
							.get(ratingResult.businessId);
					ComparableRating comparableRating = new ComparableRating();
					comparableRating.originalUserId = ratingResult.userId;
					comparableRating.userId = (String) businessReview
							.get(CollectionReview.KEY_USER_ID);
					comparableRating.stars = (Integer) businessReview
							.get(CollectionReview.KEY_STARS);
					comparableRating.coefficient = coefficient;
					/*
					 * add an entry containing the other users' actual stars
					 * rating and the similarity of the other user vs. the
					 * (original) user
					 */
					comparableRatingArray.add(comparableRating);
					System.out.println(comparableRating);
				} // if
			} // if
		} // while
		cursorReview2.close();
		/*
		 * total number of reviews of the same business by other users where
		 * similarity can be computed
		 */
		ratingResult.totalUserCount = comparableRatingArray.size();
		/* only use a subset of all other users' reviews */
		ratingResult.actualUserCount = limitComparableRating(comparableRatingArray);
		/* generate the prediction using the subset */
		ratingResult.predictedStars = computeComparableRating(
				comparableRatingArray, ratingResult.actualUserCount);
		System.out.println(ratingResult);
		if (ratingResult.predictedStars == null) {
			System.out.println("Insufficient data to predict rating");
		} else {
			double error = ratingResult.predictedStars - ratingResult.stars;
			System.out.println("error=" + error + " predictedStars="
					+ ratingResult.predictedStars + " actualStars="
					+ ratingResult.stars);
		} // else
		/* naive average rating of the (original) business by all other users */
		ratingResult.averageRatingStars = (double) totalRatingStars
				/ (double) totalRatingCount;
		double arsError = ratingResult.averageRatingStars - ratingResult.stars;
		System.out.println("averageRatingStars.error="
				+ Double.toString(arsError) + " averageRatingStars="
				+ Double.toString(ratingResult.averageRatingStars)
				+ " actualStars=" + ratingResult.stars);
		return ratingResult;
	} // predictUserRating

	/**
	 * Analyze the accuracy of each prediction and calculates the root mean
	 * square of the error of each prediction
	 * 
	 * @param ratingResultArray
	 *            Each entry is the actual and predicted stars of a review
	 *            object
	 */
	private void analyzeAccuracy(ArrayList<RatingResult> ratingResultArray) {

		double totalSquare = 0.0;
		int predictionCount = 0;
		int noPredictionCount = 0;

		double averageStarsTotalSquare = 0.0;
		int averageStarsCount = 0;

		int midPointStars = (MAXIMUM_STARS + MINIMUM_STARS) / 2;
		double midPointStarsTotalSquare = 0.0;
		int midPointStarsCount = 0;

		Iterator<RatingResult> itr = ratingResultArray.iterator();
		while (itr.hasNext()) {
			RatingResult ratingResult = itr.next();
			if (ratingResult.predictedStars == null) {
				noPredictionCount++;
			} else {
				double error = ratingResult.predictedStars - ratingResult.stars;
				totalSquare += error * error;
				predictionCount++;
			} // else
			if (ratingResult.averageRatingStars != null) {
				double error = ratingResult.averageRatingStars
						- ratingResult.stars;
				averageStarsTotalSquare += error * error;
				averageStarsCount++;
			} // if
			double error = midPointStars - ratingResult.stars;
			midPointStarsTotalSquare += error * error;
			midPointStarsCount++;
		} // while
		int totalCount = noPredictionCount + predictionCount;
		if (predictionCount == 0) {
			System.out.println("No prediction predictionPercentage=0(=0/"
					+ totalCount + ")");
		} else {
			double predictionPercentage = ((double) predictionCount)
					/ totalCount;
			double rms = Math.sqrt(totalSquare / predictionCount);
			System.out.println("RMS(" + predictionCount + ")=" + rms
					+ " predictionPercentage=" + predictionPercentage + "(="
					+ predictionCount + "/" + totalCount + ")");
		} // else
		if (averageStarsCount > 0) {
			double rms = Math.sqrt(averageStarsTotalSquare / averageStarsCount);
			System.out.println("averageStarsCount.RMS(" + averageStarsCount
					+ ")=" + rms);
		} // if
		if (midPointStarsCount > 0) {
			double rms = Math.sqrt(midPointStarsTotalSquare
					/ midPointStarsCount);
			System.out.println("midPointStarsCount.RMS(" + midPointStarsCount
					+ ")=" + rms);
		} // if
	} // summarizeResult

	/**
	 * Ensure that user id and business id have performance index
	 * 
	 * @param coll
	 */
	private void ensureCollectionReviewIndex(DBCollection coll) {
		coll.ensureIndex(CollectionReview.KEY_USER_ID);
		coll.ensureIndex(CollectionReview.KEY_BUSINESS_ID);
	} // ensureCollectionReviewIndex

	private void ensureCollectionUserIndex(DBCollection coll) {
		coll.ensureIndex(CollectionUser.KEY_USER_ID);
	} // ensureCollectionUserIndex

	/**
	 * Finds all reviews according to the filter and executes the collaborative
	 * filtering algorithm on each review object found
	 * 
	 * @param filter
	 * @throws Exception
	 */
	private void _predictRating(BasicDBObject filter) throws Exception {
		openDatabase();
		DBCollection reviewCollection = db.getCollection(COLLECTION_REVIEW);
		if (reviewCollection == null) {
			throw new Exception("Cannot get collection " + COLLECTION_REVIEW);
		}
		ensureCollectionReviewIndex(reviewCollection);
		DBCursor cursorReview = reviewCollection.find(filter);
		ArrayList<RatingResult> ratingResultArray = new ArrayList<RatingResult>();
		while (cursorReview.hasNext()) {
			DBObject reviewObject = cursorReview.next();
			/* apply collaborative filtering on the review object */
			RatingResult ratingResult = predictUserRating(reviewObject);
			if (ratingResult != null) {
				ratingResultArray.add(ratingResult);
			} // if
		} // while
		cursorReview.close();
		/* analyze the accuracy of the collaborative filtering algorithm */
		analyzeAccuracy(ratingResultArray);
	} // _predictRating

	/**
	 * 
	 * Main entry point to loop through one review object (defined by user id
	 * plus business id) and execute the collaborative filtering function
	 * 
	 * @param userId
	 * @param businessId
	 * @throws Exception
	 */
	public void predictRating(String userId, String businessId)
			throws Exception {
		System.out.println("predictRating: userId=" + userId + " businessId="
				+ businessId);
		BasicDBObject filter = new BasicDBObject();
		if (userId != null)
			filter.put(CollectionReview.KEY_USER_ID, userId);
		if (businessId != null)
			filter.put(CollectionReview.KEY_BUSINESS_ID, businessId);
		_predictRating(filter);
	} // predictRating

	/**
	 * Main entry point to loop through every review object and execute the
	 * collaborative filtering function
	 * 
	 * @throws Exception
	 */
	public void predictRating() throws Exception {
		System.out.println("predictRating");
		_predictRating(null);
	} // predictRating

} // CollaborativeFiltering

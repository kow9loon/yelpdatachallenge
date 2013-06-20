package com.apptsunami.samples.yelpdatachallenge;

import com.apptsunami.samples.yelpdatachallenge.analyze.CollaborativeFiltering;

/**
 * 
 * Main program to execute collaborative filtering on the database
 * 
 * @author stevec
 * 
 */
public class AnalyzeData {

	/**
	 * 
	 * @param args
	 * 
	 */
	public static void main(String[] args) {
		
		// final String USER_ID = "rLtl8ZkDX5vH5nAx9C3q5Q";
		// final String BUSINESS_ID = "9yKzy9PApeiPPOUJEtnvkg";
		final String USER_ID = null;
		final String BUSINESS_ID = null;
		
		System.out.println("start");
		CollaborativeFiltering recommendation = new CollaborativeFiltering();
		try {
			recommendation.openDatabase();
			if ((USER_ID != null) || (BUSINESS_ID != null)) {
				recommendation.predictRating(USER_ID, BUSINESS_ID);
			} else {
				recommendation.predictRating();
			} // else
			recommendation.closeDatabase();
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}
		System.out.println("finish");
	} // main

} // AnalyzeData

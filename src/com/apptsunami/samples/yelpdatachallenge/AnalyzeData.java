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
		System.out.println("start");
		CollaborativeFiltering recommendation = new CollaborativeFiltering();
		try {
			recommendation.openDatabase();
			// recommendation.predictRating("1ieuYcKS7zeAv_U15AB13A",
			// "hW0Ne_HTHEAgGF1rAdmR-g");
			recommendation.predictRating();
			recommendation.closeDatabase();
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}
		System.out.println("finish");
	} // main

} // AnalyzeData

package com.apptsunami.samples.yelpdatachallenge;

import java.io.File;

import com.apptsunami.samples.yelpdatachallenge.parse.ProcessJsonFile;

public class LoadData {

	/**
	 * 
	 * @param fileName
	 * @return mongo collection name
	 * 
	 * Generate a collection name based on the file name.  For example:
	 * yelp_academic_dataset_business.json => business
	 * yelp_academic_dataset_checkin.json => checkin
	 * 
	 */
	private static String getCollectionNameFromFileName(String fileName) {
		String str = fileName.substring(fileName.lastIndexOf("_")+1);
		return str.substring(0, str.indexOf("."));
	} // getCollectionNameFromFileName

	/**
	 * 
	 * @param args
	 * 
	 * Main program to load data files.  Specify each json file as an argument.
	 */
	public static void main(String[] args) {
		System.out.println("start");
		ProcessJsonFile parser = new ProcessJsonFile();
		for (int i = 0; i < args.length; i++) {
			System.out.println("loading file " + i + ": " + args[i]);
			String fileName = args[i];
			File inputFile = new File(fileName);
			try {
				parser.processFile(inputFile,
						getCollectionNameFromFileName(fileName));
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} // for
		System.out.println("finish");
	} // main

} // LoadData

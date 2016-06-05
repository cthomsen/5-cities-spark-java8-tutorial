package de.bst.five_cities.utils;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkUtils {

	private static JavaSparkContext sparkContext;

	public static JavaSparkContext getSparkContext() {
		if (sparkContext != null)
			return sparkContext;

		SparkConf sparkConf = new SparkConf();
		sparkConf.setAppName("Hallo Spark!");
		sparkConf.setMaster("local[*]");

		sparkContext = new JavaSparkContext(sparkConf);
		return sparkContext;
	}

	public static void close() {
		if (sparkContext != null) {
			sparkContext.close();
			sparkContext = null;
		}
	}

	public static <T> T debugPrint(T something) {
		System.out.println(">> " + something);
		return something;
	}

}

package org.workflowsim.crowdsourcing.util;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.math3.random.RandomDataGenerator;
public class RandomUtil{

	
	private static	RandomDataGenerator uniformRandomDataGenerator = new RandomDataGenerator();
	private static	RandomDataGenerator gaussianRandomDataGenerator = new RandomDataGenerator();
	private static	RandomDataGenerator sampleRandomDataGenerator = new RandomDataGenerator();

	static {
		long seed = 17399225432L; 
		uniformRandomDataGenerator.reSeed(seed);
		gaussianRandomDataGenerator.reSeed(seed);
		sampleRandomDataGenerator.reSeed(seed);
	}
	public static int randInt(int lower, int upper) {
		
		return uniformRandomDataGenerator.nextInt(lower, upper);
	}
	public static int randZeroOrOne() {
		List<Integer> samples= new ArrayList<Integer>();
		samples.add(0);
		samples.add(1);
		Object result[]= sampleRandomDataGenerator.nextSample(samples,1);
		return (int)result[0];
	}
	public static int randRamSize() {
		List<Integer> samples= new ArrayList<Integer>();
		samples.add(512);
		samples.add(1024);
		samples.add(2048);
		Object result[]= sampleRandomDataGenerator.nextSample(samples,1);
		return (int)result[0];
	}
	public static double nextGaussian(double lower,double upper) {
		double mu = (lower+upper)/2.0;
		double sigma = (upper-lower)/6.0;
		double random = gaussianRandomDataGenerator.nextGaussian(mu,sigma);
		
		return random;
	}
	public static int[] generateKIntegers(int lower, int upper,int k) {
		int[] rn = new int[k];
		for(int i=0; i<k; i++) {
			rn[i]=randInt(lower,upper);
		}
		return rn;
	}
	public static void main(String[]args) {
		for(int i=0; i<10000; i++) {
			double r = nextGaussian(0.1,0.9);
			if(r<0 || r>1) {
			System.out.println(r);
			}
		}
	}
}

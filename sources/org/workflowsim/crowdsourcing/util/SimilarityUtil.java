package org.workflowsim.crowdsourcing.util;

import org.workflowsim.crowdsourcing.CrowdTask;
import org.workflowsim.crowdsourcing.Worker;

public class SimilarityUtil {

	public static boolean caculateTwoWorkersSimilarity(Worker srcWorker, Worker dstWorker, double threshold) {
		
		double psrc = srcWorker.getP();
		double vsrc = srcWorker.getV();
		int[]  Ssrc = srcWorker.getS();
		double pdst = dstWorker.getP(); //[0.1,0.9]
		double vdst = dstWorker.getV();//[0.1,0.6]
		int[]  Sdst = dstWorker.getS();  
		
		// 工资差值不超过5%
		// 工人的接受概率不超过5%  
		//最大值是0.2
		//技能中至少有7项相同,最大值是3(就是有3项不同)
		double sim1 = Math.abs(psrc-pdst)+ Math.abs(vsrc-vdst);
		for(int i=0; i<Ssrc.length && i<Sdst.length; i++) {
			sim1 += Math.abs(Ssrc[i]-Sdst[i]);
		}
		//threshold = 3.1;
		threshold= 7.1;
		if(sim1<threshold) {
			return true;
		}
		
		
		return false;
	}
	
	public static boolean caculateWorkerandTaskSimilarity(Worker worker, CrowdTask crowdTask, double threshold) {
		
		
		
		int[]  workerS = worker.getS();
		
		int[]  taskS = crowdTask.getS();  
		boolean isSimilar = true;
		for(int i=0; i<workerS.length && i<taskS.length; i++) {
			if(workerS[i]< taskS[i]) {
				isSimilar = false;
				break;
			}
		}
		return isSimilar;
	}
}

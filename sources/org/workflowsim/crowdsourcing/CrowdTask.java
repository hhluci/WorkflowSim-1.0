package org.workflowsim.crowdsourcing;

import java.util.ArrayList;
import java.util.List;

import org.workflowsim.CondorVM;
import org.workflowsim.Job;
import org.workflowsim.crowdsourcing.util.RandomUtil;

public class CrowdTask extends Job{

	private int taskSolutionType;//������������ 0:machine 1:human
	private int taskBusinessType;//������������ 0:��ҳ����  1:��д����
	private List<String> conditions;// �������Ҫ��Ļ������û��˼��ܼ���
	private double reward;//�������Ļ㱨
	private double trust;//������������
	private List<CondorVM> randomList;
	private List<CondorVM> maxTrustList;
	private List<CondorVM> minCostList;
	private List<CondorVM> minuTrustCostList;
	
	/*****************fariness properties start******************/
	private int[] S= new int[10]; //��Ҫ��ļ�������
	private double v; //������ɵ�Ԥ��
	/*****************fariness properties end******************/
	public CrowdTask(int crowdTaskId, long crowdTaskLength) {
		super(crowdTaskId, crowdTaskLength);
		randomList = new ArrayList<CondorVM>();
		maxTrustList = new ArrayList<CondorVM>();
		minCostList = new ArrayList<CondorVM>();
		minuTrustCostList = new ArrayList<CondorVM>();
		taskSolutionType = 0;
		
		/********************init fairness properties start**********************/
		v = RandomUtil.nextGaussian(0.1, 0.6);
		
		
		for(int i=0; i<10; i++) {
			S[i] = 0;//RandomUtil.randZeroOrOne();
		}
		for(int i=0;i<3; i++) {
			int elementIndex = RandomUtil.randInt(0, 9);
			S[elementIndex] =  1;
		}
		/********************init fairness properties end**********************/
	}
	public void addOneRandom(CondorVM condorVM) {
		randomList.add(condorVM);
	}
	public void addOneMaxTrust(CondorVM condorVM) {
		maxTrustList.add(condorVM);
	}
	public void addOneMinCostList(CondorVM condorVM) {
		minCostList.add(condorVM);
	}
	public void addOneMinuTrustCost(CondorVM condorVM) {
		minuTrustCostList.add(condorVM);
	}
	public int getTaskSolutionType() {
		return taskSolutionType;
	}
	public void setTaskSolutionType(int taskSolutionType) {
		this.taskSolutionType = taskSolutionType;
	}
	public int getTaskBusinessType() {
		return taskBusinessType;
	}
	public void setTaskBusinessType(int taskBusinessType) {
		this.taskBusinessType = taskBusinessType;
	}
	public List<String> getConditions() {
		return conditions;
	}
	public void setConditions(List<String> conditions) {
		this.conditions = conditions;
	}
	public double getReward() {
		return reward;
	}
	public void setReward(double reward) {
		this.reward = reward;
	}
	public double getTrust() {
		return trust;
	}
	public void setTrust(double trust) {
		this.trust = trust;
	}
    /************fairness methods start****************/
	public int[] getS() {
		return S;
	}
	public void setS(int[] s) {
		S = s;
	}
	public double getV() {
		return v;
	}
	public void setV(double v) {
		this.v = v;
	}
	
	 /************fairness methods end****************/
	
}

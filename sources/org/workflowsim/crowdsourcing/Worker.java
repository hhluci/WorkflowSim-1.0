package org.workflowsim.crowdsourcing;

import org.cloudbus.cloudsim.CloudletScheduler;
import org.workflowsim.CondorVM;
import org.workflowsim.crowdsourcing.util.RandomUtil;
import org.workflowsim.failure.FailureMonitor;

public class Worker extends  CondorVM{

	private int allocatedTaskNumber; //����ù��˵���������
	private int completedTaskNumber; //�ɹ���ɵ���������
	private int rawAllocatedTaskNumber; //�������������������
	private int rawcompletedTaskNumber; //�ɹ���ɵ���������
	private double leastCost;// ���һ���������ͷ���
	private double skillMatchProbability;// ���˼����������ƥ��̶�
	private double acceptRatio; //��������ĸ���
	/************fairness properties start*******************/
	
	private double n1; // �����������������.����ù��˱���Ϊ��ѡ��,����ϵͳ�������������������
	private double n2; // �����ܵ���������.��������Ϊ��ѡ�ߵĴ���
	private double p;// ���˽�������ĸ���
	private int[] S= new int[10]; //��������
	private double v; //��������������ͱ���
	
	
	private int groupNo;
	
	/************fairness properties start*******************/
	public Worker(int id, int userId, double mips, int numberOfPes, int ram, long bw, long size, String vmm,
			CloudletScheduler cloudletScheduler) {
		super(id, userId, mips, numberOfPes, ram, bw, size, vmm, cloudletScheduler);
		allocatedTaskNumber = RandomUtil.randInt(100, 1000);
		completedTaskNumber = RandomUtil.randInt(50, allocatedTaskNumber);
		rawAllocatedTaskNumber = allocatedTaskNumber;
		rawcompletedTaskNumber = completedTaskNumber;
		this.leastCost = RandomUtil.nextGaussian(0.1, 0.6);
		this.skillMatchProbability =RandomUtil.nextGaussian(0.1, 0.9);
		this.acceptRatio = RandomUtil.nextGaussian(0.1, 0.9);
		
		/*****init fairness properties****/
		
		
		n2 = RandomUtil.randInt(10, 100);
		n1 = RandomUtil.randInt(1, (int)n2);
		p = RandomUtil.nextGaussian(0.1, 0.9);
		v = RandomUtil.nextGaussian(0.1, 0.6);
		groupNo = 0;
		for(int i=0; i<10; i++) {
			S[i] = RandomUtil.randZeroOrOne();
		}
	}
	
	public int getAllocatedTaskNumber() {
		return allocatedTaskNumber;
	}
	public void addAllocatedTaskNumber() {
		this.allocatedTaskNumber++;
	}
	public int getCompletedTaskNumber() {
		int totalFailures = FailureMonitor.getVmFaillures(this.getId());
		int tcompletedTaskNumber = allocatedTaskNumber - totalFailures  - rawAllocatedTaskNumber;
		return completedTaskNumber+tcompletedTaskNumber;
	}
	public void addCompletedTaskNumber() {
		this.completedTaskNumber++;
	}
	
	public void setAllocatedTaskNumber(int allocatedTaskNumber) {
		this.allocatedTaskNumber = allocatedTaskNumber;
	}

	public void setCompletedTaskNumber(int completedTaskNumber) {
		this.completedTaskNumber = completedTaskNumber;
	}

	public double getLeastCost() {
		return leastCost;
	}
	public void setLeastCost(double leastCost) {
		this.leastCost = leastCost;
	}

	public double getSkillMatchProbability() {
		double p = skillMatchProbability;
		skillMatchProbability = RandomUtil.nextGaussian(0.1, 0.9);
		return p;
	}

	public void setSkillMatchProbability(double skillMatchProbability) {
		this.skillMatchProbability = skillMatchProbability;
	}

	public double getAcceptRatio() {
		return acceptRatio;
	}

	public void setAcceptRatio(double acceptRatio) {
		this.acceptRatio = acceptRatio;
	}

	public int getRawAllocatedTaskNumber() {
		return rawAllocatedTaskNumber;
	}

	

	public int getRawcompletedTaskNumber() {
		return rawcompletedTaskNumber;
	}

	public double getN1() {
		return n1;
	}

	public void setN1(double n1) {
		this.n1 = n1;
	}

	public double getN2() {
		return n2;
	}

	public void setN2(double n2) {
		this.n2 = n2;
	}

	public double getP() {
		return p;
	}

	public void setP(double p) {
		this.p = p;
	}

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

	public int getGroupNo() {
		return groupNo;
	}

	public void setGroupNo(int groupNo) {
		this.groupNo = groupNo;
	}

	/*************************fairness methods start*************/
	
	
	/*************************fairness methods end*************/
}

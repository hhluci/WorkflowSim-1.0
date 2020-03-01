package org.workflowsim.crowdsourcing;

import org.cloudbus.cloudsim.CloudletScheduler;
import org.workflowsim.CondorVM;
import org.workflowsim.crowdsourcing.util.RandomUtil;
import org.workflowsim.failure.FailureMonitor;

public class Machine extends CondorVM{

	private int allocatedTaskNumber; //分配给机器的任务数量
	private int completedTaskNumber; //成功完成的任务数量
	private int rawAllocatedTaskNumber; //分配给机器的任务数量
	private int rawcompletedTaskNumber; //成功完成的任务数量
	private double leastCost;// 完成一项任务的最低费用
	private double configurationMatchProbability;// 机器配置与任务的匹配程度
	private double acceptRatio; //接受任务的概率
	public Machine(int id, int userId, double mips, int numberOfPes, int ram, long bw, long size, String vmm,
			CloudletScheduler cloudletScheduler) {
		super(id, userId, mips, numberOfPes, ram, bw, size, vmm, cloudletScheduler);
		allocatedTaskNumber = RandomUtil.randInt(100, 1000);
		completedTaskNumber = RandomUtil.randInt(50, allocatedTaskNumber);
		rawAllocatedTaskNumber = allocatedTaskNumber;
		rawcompletedTaskNumber = completedTaskNumber;
		this.leastCost = RandomUtil.nextGaussian(0.1, 0.6);
		this.configurationMatchProbability =RandomUtil.nextGaussian(0.1, 0.9);
		this.acceptRatio = RandomUtil.nextGaussian(0.1, 0.9);
	}
	
	public int getAllocatedTaskNumber() {
		return allocatedTaskNumber;
	}
	public void addAllocatedTaskNumber() {
		this.allocatedTaskNumber++;
	}
	
	
	public int getCompletedTaskNumber() {
		int totalFailures = FailureMonitor.getVmFaillures(this.getId());
		int tcompletedTaskNumber = allocatedTaskNumber - totalFailures - rawAllocatedTaskNumber;
		return completedTaskNumber+tcompletedTaskNumber;
	}
	
	public double getLeastCost() {
		return leastCost;
	}
	public void setLeastCost(double leastCost) {
		this.leastCost = leastCost;
	}
	public double getConfigurationMatchProbability() {
		double p = configurationMatchProbability;
		configurationMatchProbability = RandomUtil.nextGaussian(0.1, 0.9);
		return p;
	}
	public void setConfigurationMatchProbability(double configurationMatchProbability) {
		this.configurationMatchProbability = configurationMatchProbability;
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

	
	
	
	
}

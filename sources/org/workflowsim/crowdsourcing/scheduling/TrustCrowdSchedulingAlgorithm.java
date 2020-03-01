package org.workflowsim.crowdsourcing.scheduling;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.util.MathUtil;
import org.workflowsim.CondorVM;
import org.workflowsim.WorkflowSimTags;
import org.workflowsim.crowdsourcing.CrowdTask;
import org.workflowsim.crowdsourcing.Machine;
import org.workflowsim.crowdsourcing.Worker;
import org.workflowsim.crowdsourcing.util.RandomUtil;
import org.workflowsim.scheduling.BaseSchedulingAlgorithm;

public class TrustCrowdSchedulingAlgorithm extends BaseSchedulingAlgorithm{

	private double w0 = 0.5;
	private double w1 =0.5;
	private int k = 3;
	private double theta = 0.2;// trust ãÐÖµ
	private ALGORITHM_TYPE algorithmType = ALGORITHM_TYPE.MINUS_TRUST_COST;
	public enum ALGORITHM_TYPE{
		 RANDOM,MAX_TRUST,MIN_COST,MINUS_TRUST_COST
	}
	public TrustCrowdSchedulingAlgorithm() {
		super();
	}
	@Override
	public void run() throws Exception {
		List<Worker> Workers = new ArrayList<Worker>();
		List<Machine> Machines = new ArrayList<Machine>();
		Map<Integer, CondorVM> mId2Vm = new HashMap<>();
		
		
		int minCandidates = 10;
        for (int i = 0; i < getVmList().size(); i++) {
            CondorVM vm = (CondorVM) getVmList().get(i);
            
            if (vm != null) {
            	if(vm instanceof Worker) {
            		Worker worker = (Worker)vm;
            		worker.getCompletedTaskNumber();
            		Workers.add(worker);
            	}else if(vm instanceof Machine) {
            		Machine machine = (Machine)vm;
            		machine.getCompletedTaskNumber();
            		Machines.add(machine);
            	}
            	mId2Vm.put(vm.getId(), vm);
            }
        }

        int size = getCloudletList().size();

        for (int i = 0; i < size; i++) {
            Cloudlet cloudlet = (Cloudlet) getCloudletList().get(i);
            
            if(cloudlet instanceof CrowdTask) {
            	CrowdTask crowdTask = (CrowdTask) cloudlet;
            	if(crowdTask.getTaskSolutionType()==0) { // machine task
            		
            		if (cloudlet.getVmId() < 0 || !mId2Vm.containsKey(cloudlet.getVmId())) {
                      //  Log.printLine("Machine Cloudlet " + cloudlet.getCloudletId() + " is not matched."
                     //           + "It is possible a stage-in job");
                        List<Machine> nominess = getNomineesMachines(Machines);
                		if(nominess.size()==0) {
                			Log.printLine("Machine nominess is 0 for " + crowdTask.getCloudletId());
                			return;
                		}
                		double reward = caculateRewardByMachineNominess(nominess);
                		crowdTask.setReward(reward);
                		List<Machine> candidates = getCandidatesMachines(nominess,k,minCandidates);
                		if(candidates.size()==0) {
                			Log.printLine("Machine candidates is 0 for " + crowdTask.getCloudletId());
                			return;
                		}
                		Machine machine = getOneMachine(candidates);
                		
                		if(machine==null) {
                			Log.printLine("Selected Machine  is null for " + crowdTask.getCloudletId());
                			return;
                		}
                		double trust = caculateTrustByMachine(machine);
                		crowdTask.setTrust(trust);
                		 reward = caculateRewardBySelectedMachine(machine,crowdTask);
                 		crowdTask.setReward(reward);
                        cloudlet.setVmId(machine.getId());
                        machine.addAllocatedTaskNumber();
                    }
            		
            		CondorVM vm = mId2Vm.get(cloudlet.getVmId());
            		if (vm.getState() == WorkflowSimTags.VM_STATUS_IDLE) {
                        vm.setState(WorkflowSimTags.VM_STATUS_BUSY);
                        getScheduledList().add(cloudlet);
                        Log.printLine("Machine Schedules " + cloudlet.getCloudletId() + " with "
                                + cloudlet.getCloudletLength() + " to VM " + cloudlet.getVmId());
                    }
            	}else if(crowdTask.getTaskSolutionType()==1) { // human task
            		
            		
            		if (cloudlet.getVmId() < 0 || !mId2Vm.containsKey(cloudlet.getVmId())) {
                      //  Log.printLine("Worker Cloudlet " + cloudlet.getCloudletId() + " is not matched."
                      //          + "It is possible a stage-in job");
                        List<Worker> nominess = getNomineesWorkers(Workers);
                		if(nominess.size()==0) {
                			Log.printLine("Worker nominess is 0 for " + crowdTask.getCloudletId());
                			return;
                		}
                		double reward = caculateRewardByWorkerNominess(nominess);
                		crowdTask.setReward(reward);
                		List<Worker> candidates = getCandidatesWorkers(nominess,k,minCandidates);
                		if(candidates.size()==0) {
                			Log.printLine("Worker candidates is 0 for " + crowdTask.getCloudletId());
                			return;
                		}
                		Worker worker = getOneWorker(candidates);
                		if(worker==null) {
                			Log.printLine("Selected Worker  is null for " + crowdTask.getCloudletId());
                			return;
                		}
                		double trust = caculateTrustByWorker(worker);
                		crowdTask.setTrust(trust);
                	     reward = caculateRewardBySelectedWorker(worker,crowdTask);
                		crowdTask.setReward(reward);
                        cloudlet.setVmId(worker.getId());
                        worker.addAllocatedTaskNumber();
                    }
            		
            		CondorVM vm = mId2Vm.get(cloudlet.getVmId());
            		if (vm.getState() == WorkflowSimTags.VM_STATUS_IDLE) {
                        vm.setState(WorkflowSimTags.VM_STATUS_BUSY);
                        getScheduledList().add(cloudlet);
                        Log.printLine("Worker Schedules " + cloudlet.getCloudletId() + " with "
                                + cloudlet.getCloudletLength() + " to VM " + cloudlet.getVmId());
                    }
            	}else {
               	 /**
                     * Make sure cloudlet is matched to a VM. It should be done in the
                     * Workflow Planner. If not, throws an exception because
                     * StaticSchedulingAlgorithm itself does not do the mapping.
                     */
                    if (cloudlet.getVmId() < 0 || !mId2Vm.containsKey(cloudlet.getVmId())) {
                        Log.printLine("Cloudlet " + cloudlet.getCloudletId() + " is not matched."
                                + "It is possible a stage-in job");
                        cloudlet.setVmId(0);

                    }
                		CondorVM vm = mId2Vm.get(cloudlet.getVmId());
                        if (vm.getState() == WorkflowSimTags.VM_STATUS_IDLE) {
                            vm.setState(WorkflowSimTags.VM_STATUS_BUSY);
                            getScheduledList().add(cloudlet);
                            Log.printLine("Static Schedules " + cloudlet.getCloudletId() + " with "
                                    + cloudlet.getCloudletLength() + " to VM " + cloudlet.getVmId());
                        }
                	}
            }
           
            
            
        }
		
	}
	private double caculateTrustByMachine(Machine machine) {
		int completedTaskNumber = machine.getCompletedTaskNumber();
		int allocatedTaskNumber = machine.getAllocatedTaskNumber();
		double p0 = (double)completedTaskNumber / (double)allocatedTaskNumber;
		double p1 = machine.getConfigurationMatchProbability();
		double score = w0 * p0 + w1 * p1;
		return score;
	}
	private double caculateTrustByWorker(Worker worker) {
		int completedTaskNumber = worker.getCompletedTaskNumber();
		int allocatedTaskNumber = worker.getAllocatedTaskNumber();
		double p0 = (double)completedTaskNumber / (double)allocatedTaskNumber;
		double p1 = worker.getSkillMatchProbability();
		double score = w0 * p0 + w1 * p1;
		return score;
	}
	private double caculateRewardBySelectedWorker(Worker worker,CrowdTask crowdTask) {
		double reward = 0.0;
		reward = (worker.getLeastCost() + crowdTask.getReward())/2.0;
		return reward;
	}
	private double caculateRewardBySelectedMachine(Machine machine,CrowdTask crowdTask) {
		double reward = 0.0;
		reward = (machine.getLeastCost() + crowdTask.getReward())/2.0;
		return reward;
	}
	private  Machine getOneMachine(List<Machine> candidates) {
		Machine machine = null;
		switch(algorithmType) {
			case RANDOM: machine = getOneMachineByRandom(candidates);break;
			case MAX_TRUST: machine = getOneMachineByTrust(candidates);break;
			case MIN_COST: machine = getOneMachineByCost(candidates);break;
			case MINUS_TRUST_COST:machine = getOneMachineByMinusTrustCost(candidates);break;
		}
		
		return machine;
	}
	
	private  Worker getOneWorker(List<Worker> candidates) {
		Worker worker = null;
		switch(algorithmType) {
			case RANDOM: worker = getOneWorkerByRandom(candidates);break;
			case MAX_TRUST: worker = getOneWorkerByTrust(candidates);break;
			case MIN_COST: worker = getOneWorkerByCost(candidates);break;
			case MINUS_TRUST_COST:worker = getOneWorkerByMinusTrustCost(candidates);break;
		}
		
		return worker;
	}
	
	private Machine getOneMachineByMinusTrustCost(List<Machine> candidates) {
		Machine machine = null;
		if(candidates.size()==1) {
			machine = candidates.get(0);
		}
		double trust_cost = 0.0;
		for(Machine obj: candidates) {
			int completedTaskNumber = obj.getCompletedTaskNumber();
			int allocatedTaskNumber = obj.getAllocatedTaskNumber();
			double p0 = (double)completedTaskNumber / (double)allocatedTaskNumber;
			double p1 = obj.getConfigurationMatchProbability();
			double score = w0 * p0 + w1 * p1;
			
			if((score-obj.getLeastCost())>trust_cost) {
				trust_cost = score-obj.getLeastCost();
				machine = obj;
			}
		}
		return machine;
	}
	private Worker getOneWorkerByMinusTrustCost(List<Worker> candidates) {
		Worker worker = null;
		if(candidates.size()==1) {
			worker = candidates.get(0);
		}
		double trust_cost = 0.0;
		for(Worker obj: candidates) {
			int completedTaskNumber = obj.getCompletedTaskNumber();
			int allocatedTaskNumber = obj.getAllocatedTaskNumber();
			double p0 = (double)completedTaskNumber / (double)allocatedTaskNumber;
			double p1 = obj.getSkillMatchProbability();
			double score = w0 * p0 + w1 * p1;
			
			if((score-obj.getLeastCost())>trust_cost) {
				trust_cost = score-obj.getLeastCost();
				worker = obj;
			}
		}
		return worker;
	}
	private Machine getOneMachineByCost(List<Machine> candidates) {
		Machine machine = null;
		if(candidates.size()==1) {
			machine = candidates.get(0);
		}
		double cost = Double.MAX_VALUE;
		for(Machine obj: candidates) {
			
			if(obj.getLeastCost()<cost) {
				cost = obj.getLeastCost();
				machine = obj;
			}
		}
		return machine;
	}
	private Worker getOneWorkerByCost(List<Worker> candidates) {
		Worker worker = null;
		if(candidates.size()==1) {
			worker = candidates.get(0);
		}
		double cost = Double.MAX_VALUE;
		for(Worker obj: candidates) {
			
			if(obj.getLeastCost()<cost) {
				cost = obj.getLeastCost();
				worker = obj;
			}
		}
		return worker;
	}
	private Machine getOneMachineByTrust(List<Machine> candidates) {
		Machine machine = null;
		if(candidates.size()==1) {
			machine = candidates.get(0);
		}
		double trust = 0.0;
		for(Machine obj: candidates) {
			int completedTaskNumber = obj.getCompletedTaskNumber();
			int allocatedTaskNumber = obj.getAllocatedTaskNumber();
			double p0 = (double)completedTaskNumber / (double)allocatedTaskNumber;
			double p1 = obj.getConfigurationMatchProbability();
			double score = w0 * p0 + w1 * p1;
			if(score>trust) {
				trust = score;
				machine = obj;
			}
		}
		return machine;
	}
	private Worker getOneWorkerByTrust(List<Worker> candidates) {
		Worker worker = null;
		if(candidates.size()==1) {
			worker = candidates.get(0);
		}
		double trust = 0.0;
		for(Worker obj: candidates) {
			int completedTaskNumber = obj.getCompletedTaskNumber();
			int allocatedTaskNumber = obj.getAllocatedTaskNumber();
			double p0 = (double)completedTaskNumber / (double)allocatedTaskNumber;
			double p1 = obj.getSkillMatchProbability();
			double score = w0 * p0 + w1 * p1;
			if(score>trust) {
				trust = score;
				worker = obj;
			}
		}
		return worker;
	}
	private Machine getOneMachineByRandom(List<Machine> candidates) {
		if(candidates.size()==1) {
			return candidates.get(0);
		}
		int cindex = RandomUtil.randInt(0, candidates.size()-1);
		return candidates.get(cindex);
	}
	private Worker getOneWorkerByRandom(List<Worker> candidates) {
		if(candidates.size()==1) {
			return candidates.get(0);
		}
		int cindex = RandomUtil.randInt(0, candidates.size()-1);
		return candidates.get(cindex);
	}
	private List<Machine> getNomineesMachines(List<Machine> machines){
		List<Machine> nominess = new ArrayList<Machine>();
		
		for(Machine machine: machines) {
			int completedTaskNumber = machine.getCompletedTaskNumber();
			int allocatedTaskNumber = machine.getAllocatedTaskNumber();
			double p0 = (double)completedTaskNumber / (double)allocatedTaskNumber;
			double p1 = machine.getConfigurationMatchProbability();
			double score = w0 * p0 + w1 * p1;
			if(score>theta) {
				//Log.printLine("machine nominess: " + machine.getId()+",score="+score);
				nominess.add(machine);
			}
		}
		return nominess;
	}
	private List<Machine> getCandidatesMachines(List<Machine> nominess,int k, int minCandidates){
		List<Machine> candidates = new ArrayList<Machine>();
		if(nominess.size()<=minCandidates) {
			for(Machine machine: nominess) {
				candidates.add(machine);
			}
		}else {
		while(candidates.size()< minCandidates) {
			int[] cindex = RandomUtil.generateKIntegers(0, nominess.size()-1, k);
			for(int i=0; i<cindex.length; i++) {
				Machine machine = nominess.get(cindex[i]);
				double acceptRatio = machine.getAcceptRatio();
				int ar = RandomUtil.randInt(1, 100);
				if(ar>0 && ar<acceptRatio*100) {
					candidates.add(machine);
				}
			}
		}
		}
		return candidates;
	}

	private List<Worker> getNomineesWorkers(List<Worker> workers){
		List<Worker> nominess = new ArrayList<Worker>();
		
		for(Worker worker: workers) {
			int completedTaskNumber = worker.getCompletedTaskNumber();
			int allocatedTaskNumber = worker.getAllocatedTaskNumber();
			double p0 = (double)completedTaskNumber / (double)allocatedTaskNumber;
			double p1 = worker.getSkillMatchProbability();
			double score = w0 * p0 + w1 * p1;
			if(score>theta) {
				//Log.printLine("worker nominess: " + worker.getId()+",score="+score);
				nominess.add(worker);
			}
		}
		return nominess;
	}
	private List<Worker> getCandidatesWorkers(List<Worker> nominess,int k, int minCandidates){
		List<Worker> candidates = new ArrayList<Worker>();
		if(nominess.size()<=minCandidates) {
			for(Worker worker: nominess) {
				candidates.add(worker);
			}
		}else {
			while(candidates.size()<minCandidates) {
				int[] cindex = RandomUtil.generateKIntegers(0, nominess.size()-1, k);
				for(int i=0; i<cindex.length; i++) {
					Worker worker = nominess.get(cindex[i]);
					double acceptRatio = worker.getAcceptRatio();
					int ar = RandomUtil.randInt(1, 100);
					if(ar>0 && ar<acceptRatio*100) {
						candidates.add(worker);
					}
				}
			}
		}
		return candidates;
	}

	private double caculateRewardByWorkerNominess(List<Worker> nominess) {
		List<Double> rewards = new ArrayList<Double>();
		for(Worker worker: nominess) {
			rewards.add(worker.getLeastCost());
		}
		double avgReward = 0.0;
		avgReward = MathUtil.mean(rewards);
		return avgReward;
	}
	private double caculateRewardByMachineNominess(List<Machine> nominess) {
		List<Double> rewards = new ArrayList<Double>();
		for(Machine machine: nominess) {
			rewards.add(machine.getLeastCost());
		}
		double avgReward = 0.0;
		avgReward = MathUtil.mean(rewards);
		return avgReward;
	}
}

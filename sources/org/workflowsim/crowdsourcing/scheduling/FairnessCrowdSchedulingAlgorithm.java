package org.workflowsim.crowdsourcing.scheduling;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Log;
import org.workflowsim.CondorVM;
import org.workflowsim.WorkflowSimTags;
import org.workflowsim.crowdsourcing.CrowdTask;
import org.workflowsim.crowdsourcing.Worker;
import org.workflowsim.crowdsourcing.util.RandomUtil;
import org.workflowsim.crowdsourcing.util.SimilarityUtil;
import org.workflowsim.scheduling.BaseSchedulingAlgorithm;

public class FairnessCrowdSchedulingAlgorithm extends BaseSchedulingAlgorithm {

	private static ALGORITHM_TYPE algorithmType = ALGORITHM_TYPE.RANDOM;

	public enum ALGORITHM_TYPE {
		RANDOM, MAX_FAIRNESS, MIN_COST, MINUS_FAIRNESS_COST
	}

	public static String getAlgorithmType() {
		String algorithm = "";
		switch (algorithmType) {
		case RANDOM:
			algorithm = "randome";
			break;
		case MAX_FAIRNESS:
			algorithm = "maxfairness";
			break;
		case MIN_COST:
			algorithm = "mincost";
			break;
		case MINUS_FAIRNESS_COST:
			algorithm = "minusfairnesscost";
			break;
		}
		return algorithm;
	}

	public FairnessCrowdSchedulingAlgorithm() {
		super();
	}

	@Override
	public void run() throws Exception {
		List<Worker> workers = new ArrayList<Worker>();
		Map<Integer, CondorVM> mId2Vm = new HashMap<>();
		int minCandidates = 10;
		for (int i = 0; i < getVmList().size(); i++) {
			CondorVM vm = (CondorVM) getVmList().get(i);
			if (vm != null) {
				if (vm instanceof Worker) {
					Worker worker = (Worker) vm;
					worker.getCompletedTaskNumber();
					workers.add(worker);
				}
				mId2Vm.put(vm.getId(), vm);
			}
		}
		int size = getCloudletList().size();
		for (int i = 0; i < size; i++) {
			Cloudlet cloudlet = (Cloudlet) getCloudletList().get(i);
			if (cloudlet instanceof CrowdTask) {
				CrowdTask crowdTask = (CrowdTask) cloudlet;
				if (cloudlet.getVmId() < 0 || !mId2Vm.containsKey(cloudlet.getVmId())) {

					Worker worker = allocateOneWoker(workers, crowdTask);
					worker.setN1(worker.getN1() + 1);
					crowdTask.setV(worker.getV());
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
			}
		}
	}

	private Worker allocateOneWoker(List<Worker> workers, CrowdTask crowdTask) {
		int minCandidates = 3;
		List<Worker> nominess = getNomineesWorkers(workers, crowdTask); // 得提名者
		Map<Integer, List<Worker>> groupNominess = groupNominees(nominess);// 对提名者进行分组
		Map<Integer, List<Worker>> groupCandidates = groupCandidates(groupNominess, minCandidates); // 得到分组的候选工人
		Map<Integer, List<Worker>> groupCandidatesAfteraddN1 = groupCandidatesIncOnebyN1(groupCandidates);
		// 给候选工人的接受任务数量加1,成为候选意味着该工人愿意接受该任务
		Worker worker = null;
		switch (algorithmType) {
		case RANDOM:
			worker = getOneWorkerByRandom(groupCandidatesAfteraddN1);
			break;
		case MAX_FAIRNESS:
			worker = getOneWorkerByFairness(groupCandidatesAfteraddN1);
			break;
		case MIN_COST:
			worker = getOneWorkerByCost(groupCandidatesAfteraddN1);
			break;
		case MINUS_FAIRNESS_COST:
			worker = getOneWorkerByMinusFairnessCost(groupCandidatesAfteraddN1);
			break;
		}

		return worker;
	}

	private Worker getOneWorkerByRandom(Map<Integer, List<Worker>> groupCandidates) {
		List<Worker> candidates = new ArrayList<Worker>();
		for (Integer groupNo : groupCandidates.keySet()) {
			candidates.addAll(groupCandidates.get(groupNo));
		}
		int cindex = 0;
		if (candidates.size() == 1) {
			cindex = 0;
		} else if (candidates.size() == 2) {
			cindex = RandomUtil.randZeroOrOne();
		} else {
			cindex = RandomUtil.randInt(0, candidates.size() - 1);
		}

		Worker worker = candidates.get(cindex);
		return candidates.get(cindex);
	}

	private Worker getOneWorkerByFairness(Map<Integer, List<Worker>> groupCandidates) {
		double fairness[] = new double[groupCandidates.keySet().size()];
		int i = 0;
		for (Integer groupNo : groupCandidates.keySet()) {
			List<Worker> groupCandidate = groupCandidates.get(groupNo);
			double sum = 0;
			for (Worker worker : groupCandidate) {
				sum += worker.getN1() / worker.getN2();
			}
			double mean = sum / groupCandidate.size();

			double sum1 = 0;
			for (Worker worker : groupCandidate) {
				sum1 += Math.pow((mean - worker.getN1() / worker.getN2()), 2);
			}
			if (groupCandidate.size() == 1) {
				fairness[i++] = sum;
			} else {
				fairness[i++] = Math.sqrt(sum1 / (groupCandidate.size() - 1));
			}
		}

		double fairnessmin = Double.MIN_VALUE;
		int groupindex = 0;
		for (i = 0; i < fairness.length; i++) {
			if (fairness[i] > fairnessmin) {
				fairnessmin = fairness[i];
				groupindex = i;
			}
		}
		i = 0;
		Worker one = null; // 最后找出来的工人
		for (Integer groupNo : groupCandidates.keySet()) {
			List<Worker> groupCandidate = groupCandidates.get(groupNo);
			if (i == groupindex) { // 发现了公平性最差的组
				double min = Double.MAX_VALUE;
				for (Worker worker : groupCandidate) {
					double f1 = worker.getN1() / worker.getN2();
					if (f1 < min) {
						min = f1;
						one = worker;
					}

				}
				break;
			}
			i++;
		}
		return one;
	}

	private Worker getOneWorkerByCost(Map<Integer, List<Worker>> groupCandidates) {
		List<Worker> candidates = new ArrayList<Worker>();
		for (Integer groupNo : groupCandidates.keySet()) {
			candidates.addAll(groupCandidates.get(groupNo));
		}
		double cost = Double.MAX_VALUE;
		Worker worker = null;
		for (Worker obj : candidates) {

			if (obj.getV() < cost) {
				cost = obj.getV();
				worker = obj;
			}
		}
		return worker;
	}

	private Worker getOneWorkerByMinusFairnessCost(Map<Integer, List<Worker>> groupCandidates) {
		double fairness[] = new double[groupCandidates.keySet().size()];
		double cost[] = new double[groupCandidates.keySet().size()];
		int i = 0;
		for (Integer groupNo : groupCandidates.keySet()) {
			List<Worker> groupCandidate = groupCandidates.get(groupNo);
			double sum = 0;
			double meanCost = 0.0;
			for (Worker worker : groupCandidate) {
				sum += worker.getN1() / worker.getN2();
				meanCost += worker.getV();
			}
			double mean = sum / groupCandidate.size();

			double sum1 = 0;
			for (Worker worker : groupCandidate) {
				sum1 += Math.pow((mean - worker.getN1() / worker.getN2()), 2);
			}
			if (groupCandidate.size() == 1) {
				fairness[i] = sum;
			} else {
				fairness[i] = Math.sqrt(sum1 / (groupCandidate.size() - 1));
			}
			cost[i] = meanCost / groupCandidate.size();
			i++;
		}
		i = 0;
		/*
		 * for(Integer groupNo: groupCandidates.keySet()) { List<Worker> groupCandidate
		 * = groupCandidates.get(groupNo);
		 * 
		 * //double min = Double.MAX_VALUE; double meanCost = 0.0; for(Worker worker:
		 * groupCandidate) {
		 * 
		 * double f1 = worker.getV(); if(f1<min) { min = f1;
		 * 
		 * }
		 * 
		 * meanCost += worker.getV(); }
		 * 
		 * cost[i++] = meanCost/groupCandidates.size();
		 * 
		 * }
		 */
		double fairness_cost_min = Double.MIN_VALUE; // 值越大越不公平
		double groupindex = 0;
		for (i = 0; i < fairness.length; i++) {
			if ((fairness[i] - cost[i]) > fairness_cost_min) {
				fairness_cost_min = fairness[i] - cost[i];
				groupindex = i;
			}
		}
		i = 0;
		Worker one = null; // 最后找出来的工人
		for (Integer groupNo : groupCandidates.keySet()) {
			List<Worker> groupCandidate = groupCandidates.get(groupNo);
			if (i == groupindex) { // 发现了公平性最差的组
				double min = Double.MAX_VALUE;
				for (Worker worker : groupCandidate) {
					double f1 = worker.getN1() / worker.getN2();
					if ((f1+ worker.getV()) < min) {
						min = f1 + worker.getV();
						one = worker;
					}

				}
				break;
			}
			i++;
		}
		return one;
	}

	// 下面几个方法用于得到分组的候选工人
	private List<Worker> getNomineesWorkers(List<Worker> workers, CrowdTask crowdTask) {
		List<Worker> nominess = new ArrayList<Worker>();
		for (Worker worker : workers) {
			boolean flag = SimilarityUtil.caculateWorkerandTaskSimilarity(worker, crowdTask, 0.5);
			if (flag) {
				worker.setGroupNo(0);// 重置组号,表示未分组
				nominess.add(worker);
			}
		}
		// 如果发现不了该任务的提名者,则将所有工人作为提名者
		if (nominess.size() == 0) {
			for (Worker worker : workers) {
				worker.setGroupNo(0);// 重置组号,表示未分组
				nominess.add(worker);
			}
		}
		return nominess;
	}

	// 对提名者进行分组
	private Map<Integer, List<Worker>> groupNominees(List<Worker> workers) {
		Map<Integer, List<Worker>> groups = new HashMap<Integer, List<Worker>>();
		for (int i = 0; i < workers.size(); i++) {
			Integer groupNo = new Integer(i + 1);
			List<Worker> gworkers = new ArrayList<Worker>();
			Worker currentWorker = workers.get(i);

			if (currentWorker.getGroupNo() != 0) {
				continue;
			} else {
				for (int j = i + 1; j < workers.size(); j++) {
					boolean flag = SimilarityUtil.caculateTwoWorkersSimilarity(currentWorker, workers.get(j), 0.6);
					if (flag && (workers.get(j).getGroupNo() == 0)) {
						workers.get(j).setGroupNo(groupNo);
						gworkers.add(workers.get(j));
					}
				}

			}
			currentWorker.setGroupNo(groupNo);
			gworkers.add(currentWorker);
			groups.put(groupNo, gworkers);
		}
		return groups;
	}

	// 得到接受该任务的工人集合
	private Map<Integer, List<Worker>> groupCandidates(Map<Integer, List<Worker>> groupNomniess, int minCandidates) {
		Map<Integer, List<Worker>> groupCandidates = new HashMap<Integer, List<Worker>>();
		for (Integer groupNo : groupNomniess.keySet()) {
			List<Worker> groupCandidate = new ArrayList<Worker>();
			if (groupNomniess.get(groupNo).size() <= minCandidates) {
				for (Worker worker : groupNomniess.get(groupNo)) {

					groupCandidate.add(worker);

				}
			} else {
				while (groupCandidate.size() < minCandidates) {
					for (Worker worker : groupNomniess.get(groupNo)) {
						double acceptRatio = worker.getAcceptRatio();
						int ar = RandomUtil.randInt(1, 100);
						if (ar > 0 && ar < acceptRatio * 100) {
							groupCandidate.add(worker);
						}
					}
				}
			}
			groupCandidates.put(groupNo, groupCandidate);
		}
		return groupCandidates;
	}

	// 给候选工人的接受任务数量增加1
	private Map<Integer, List<Worker>> groupCandidatesIncOnebyN1(Map<Integer, List<Worker>> groupNomniess) {
		Map<Integer, List<Worker>> groupCandidates = new HashMap<Integer, List<Worker>>();
		for (Integer groupNo : groupNomniess.keySet()) {
			List<Worker> groupCandidate = new ArrayList<Worker>();
			for (Worker worker : groupNomniess.get(groupNo)) {
				worker.setN2(worker.getN2() + 1);
				groupCandidate.add(worker);

			}
			groupCandidates.put(groupNo, groupCandidate);
		}
		return groupCandidates;
	}

}

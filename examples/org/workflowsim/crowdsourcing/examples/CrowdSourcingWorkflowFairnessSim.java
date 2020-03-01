package org.workflowsim.crowdsourcing.examples;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.CloudletSchedulerSpaceShared;
import org.cloudbus.cloudsim.DatacenterCharacteristics;
import org.cloudbus.cloudsim.HarddriveStorage;
import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Pe;
import org.cloudbus.cloudsim.Storage;
import org.cloudbus.cloudsim.VmAllocationPolicySimple;
import org.cloudbus.cloudsim.VmSchedulerTimeShared;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.provisioners.BwProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;
import org.cloudbus.cloudsim.util.ExecutionTimeMeasurer;
import org.workflowsim.CondorVM;
import org.workflowsim.Job;
import org.workflowsim.WorkflowDatacenter;
import org.workflowsim.WorkflowEngine;
import org.workflowsim.crowdsourcing.CrowdTask;
import org.workflowsim.crowdsourcing.CrowdWorkflowPlanner;
import org.workflowsim.crowdsourcing.Machine;
import org.workflowsim.crowdsourcing.Worker;
import org.workflowsim.crowdsourcing.scheduling.FairnessCrowdSchedulingAlgorithm;
import org.workflowsim.crowdsourcing.util.DateUtil;
import org.workflowsim.crowdsourcing.util.JobComparator;
import org.workflowsim.crowdsourcing.util.RandomUtil;
import org.workflowsim.crowdsourcing.util.SimilarityUtil;
import org.workflowsim.failure.FailureGenerator;
import org.workflowsim.failure.FailureMonitor;
import org.workflowsim.failure.FailureParameters;
import org.workflowsim.utils.ClusteringParameters;
import org.workflowsim.utils.DistributionGenerator;
import org.workflowsim.utils.OverheadParameters;
import org.workflowsim.utils.Parameters;
import org.workflowsim.utils.Parameters.ClassType;
import org.workflowsim.utils.ReplicaCatalog;

public class CrowdSourcingWorkflowFairnessSim {

	private static FileOutputStream fos;
	private static  int workerNum = 20; //number of workers
    private static  int hostNumber = 14;
	static {
		File file = new File("c://out_"+DateUtil.getTodayDate()+"_"+FairnessCrowdSchedulingAlgorithm.getAlgorithmType()+".csv");
		try {
			 fos = new FileOutputStream(file);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//fos = null;
	
	}
	
	
	protected static List<CondorVM> createWorkers(int userId, int startIndex, int vms) {
        //Creates a container to store VMs. This list is passed to the broker later
        LinkedList<CondorVM> list = new LinkedList<>();

        //VM Parameters
        long size = 10000; //image size (MB)
        int ram = 512; //vm memory (MB)
        int mips = 1000;
        long bw = 1000;
        int pesNumber = 1; //number of cpus
        String vmm = "Xen"; //VMM name

        //create VMs
       
        for (int i = startIndex; i < startIndex+vms; i++) {
            double ratio = 1.0;
            
            CondorVM vm = new Worker(i, userId, mips * ratio, pesNumber, ram, bw, size, 
            		vmm, new CloudletSchedulerSpaceShared()
            		);
            list.add(vm);
        }
        return list;
    }
    ////////////////////////// STATIC METHODS ///////////////////////
    /**
     * Creates main() to run this example This example has only one datacenter
     * and one storage
     */
    public static void main(String[] args) {
        try {
            // First step: Initialize the WorkflowSim package. 
            /**
             * However, the exact number of vms may not necessarily be vmNum If
             * the data center or the host doesn't have sufficient resources the
             * exact vmNum would be smaller than that. Take care.
             */
           
        	
        	
           
            /**
             * Should change this based on real physical path
             */
            //String daxPath = "/Users/weiweich/NetBeansProjects/WorkflowSim-1.0/config/dax/Montage_100.xml";
            String daxPath = "F:\\software\\hhluci\\eclipseworkspace\\WorkflowSim-1.0\\config\\dax\\Sipht_1000.xml";
            File daxFile = new File(daxPath);
            
            if (!daxFile.exists()) {
                Log.printLine("Warning: Please replace daxPath with the physical path in your working environment!");
                return;
            }
            /*
             *  Fault Tolerant Parameters
             */
            /**
             * MONITOR_JOB classifies failures based on the level of jobs;
             * MONITOR_VM classifies failures based on the vm id; MOINTOR_ALL
             * does not do any classification; MONITOR_NONE does not record any
             * failiure.
             */
            FailureParameters.FTCMonitor ftc_monitor = FailureParameters.FTCMonitor.MONITOR_VM;
            /**
             * Similar to FTCMonitor, FTCFailure controls the way how we
             * generate failures.
             */
            FailureParameters.FTCFailure ftc_failure = FailureParameters.FTCFailure.FAILURE_NONE;//FAILURE_VM_TRUST;////FAILURE_NONE;//FAILURE_ALL;//
            /**
             * In this example, we have no clustering and thus it is no need to
             * do Fault Tolerant Clustering. By default, WorkflowSim will just
             * rety all the failed task.
             */
            FailureParameters.FTCluteringAlgorithm ftc_method = FailureParameters.FTCluteringAlgorithm.FTCLUSTERING_NOOP;
           
            /**
             * Task failure rate for each level
             *
             */
            int maxLevel = 11;
            DistributionGenerator[][] failureGenerators = new DistributionGenerator[workerNum][maxLevel];
            /**
             * Model failures as a function of VM (each VM has its own
             * independent distribution
             */
            for (int vmId = 0; vmId < workerNum; vmId++) {
                DistributionGenerator generator = new DistributionGenerator(DistributionGenerator.DistributionFamily.WEIBULL,
                        100, 1.0, 30, 300, 0.78);
                for (int level = 0; level < maxLevel; level++) {
                    failureGenerators[vmId][level] = generator;
                }
            }
            /**
             * Since we are using MINMIN scheduling algorithm, the planning
             * algorithm should be INVALID such that the planner would not
             * override the result of the scheduler
             */
            Parameters.SchedulingAlgorithm sch_method = Parameters.SchedulingAlgorithm.CROWD_FAIRNESS;
            Parameters.PlanningAlgorithm pln_method = Parameters.PlanningAlgorithm.INVALID;
            ReplicaCatalog.FileSystem file_system = ReplicaCatalog.FileSystem.SHARED;

            /**
             * No overheads
             */
            OverheadParameters op = new OverheadParameters(0, null, null, null, null, 0);

            /**
             * No Clustering
             */
            ClusteringParameters.ClusteringMethod method = ClusteringParameters.ClusteringMethod.NONE;
            ClusteringParameters cp = new ClusteringParameters(0, 0, method, null);

            /**
             * Initialize static parameters
             */
            FailureParameters.init(ftc_method, ftc_monitor, ftc_failure, failureGenerators);
            Parameters.init(workerNum, daxPath, null,
                    null, op, cp, sch_method, pln_method,
                    null, 0);
            ReplicaCatalog.init(file_system);
            FailureMonitor.init();
            FailureGenerator.init();
           
            // before creating any entities.
            int num_user = 1;   // number of grid users
            Calendar calendar = Calendar.getInstance();
            boolean trace_flag = false;  // mean trace events

            // Initialize the CloudSim library
            CloudSim.init(num_user, calendar, trace_flag);

            WorkflowDatacenter datacenter0 = createDatacenter("Datacenter_0");

            /**
             * Create a WorkflowPlanner with one schedulers.
             */
            CrowdWorkflowPlanner wfPlanner = new CrowdWorkflowPlanner("planner_0", 1);
            /**
             * Create a WorkflowEngine.
             */
            WorkflowEngine wfEngine = wfPlanner.getWorkflowEngine();
            
          
            List<CondorVM> workerlist = createWorkers(wfEngine.getSchedulerId(0),0,workerNum);
            printVmList(workerlist);
            /**
             * Create a list of VMs.The userId of a vm is basically the id of
             * the scheduler that controls this vm.
             */
            List<CondorVM> vmlist0 =  new ArrayList<CondorVM>();//createVM(wfEngine.getSchedulerId(0), Parameters.getVmNum());
           
            vmlist0.addAll(workerlist);
            
            /**
             * Submits this list of vms to this WorkflowEngine.
             */
            wfEngine.submitVmList(vmlist0, 0);

            /**
             * Binds the data centers with the scheduler.
             */
            wfEngine.bindSchedulerDatacenter(datacenter0.getId(), 0);
            ExecutionTimeMeasurer.start("crowd");
            CloudSim.startSimulation();
            List<Job> outputList0 = wfEngine.getJobsReceivedList();
           
            
            CloudSim.stopSimulation();
           
            double runTime =  ExecutionTimeMeasurer.end("crowd");
            printJobList(outputList0);
            
            Log.setOutput(fos);
            Log.printLine();
            Log.printLine("========== RunTime ==========");
            Log.printLine("runtime:"+runTime);
            Log.setOutput(null);
            printVmList(vmlist0);
            printTotalCost(outputList0);
        } catch (Exception e) {
            Log.printLine("The simulation has been terminated due to an unexpected error");
        }
    }

    protected static WorkflowDatacenter createDatacenter(String name) {

        // Here are the steps needed to create a PowerDatacenter:
        // 1. We need to create a list to store one or more
        //    Machines
        List<Host> hostList = new ArrayList<>();

        // 2. A Machine contains one or more PEs or CPUs/Cores. Therefore, should
        //    create a list to store these PEs before creating
        //    a Machine.
        int hostId = 0;
        for (int i = 1; i <= hostNumber; i++) {
            List<Pe> peList1 = new ArrayList<>();
            int mips = RandomUtil.randInt(1000, 2000);
            // 3. Create PEs and add these into the list.
            //for a quad-core machine, a list of 4 PEs is required:
            peList1.add(new Pe(0, new PeProvisionerSimple(mips))); // need to store Pe id and MIPS Rating
            peList1.add(new Pe(1, new PeProvisionerSimple(mips)));

            
            int ram = RandomUtil.randRamSize(); //host memory (MB)
            long storage = RandomUtil.randInt(500000, 1000000); //host storage
            int bw = 10000;
            hostList.add(
                    new Host(
                            hostId,
                            new RamProvisionerSimple(ram),
                            new BwProvisionerSimple(bw),
                            storage,
                            peList1,
                            new VmSchedulerTimeShared(peList1))); // This is our first machine
            hostId++;
        }

        // 4. Create a DatacenterCharacteristics object that stores the
        //    properties of a data center: architecture, OS, list of
        //    Machines, allocation policy: time- or space-shared, time zone
        //    and its price (G$/Pe time unit).
        String arch = "x86";      // system architecture
        String os = "Linux";          // operating system
        String vmm = "Xen";
        double time_zone = 10.0;         // time zone this resource located
        double cost = 3.0;              // the cost of using processing in this resource
        double costPerMem = 0.05;		// the cost of using memory in this resource
        double costPerStorage = 0.1;	// the cost of using storage in this resource
        double costPerBw = 0.1;			// the cost of using bw in this resource
        LinkedList<Storage> storageList = new LinkedList<>();	//we are not adding SAN devices by now
        WorkflowDatacenter datacenter = null;

        DatacenterCharacteristics characteristics = new DatacenterCharacteristics(
                arch, os, vmm, hostList, time_zone, cost, costPerMem, costPerStorage, costPerBw);

        // 5. Finally, we need to create a storage object.
        /**
         * The bandwidth within a data center in MB/s.
         */
        int maxTransferRate = 15;// the number comes from the futuregrid site, you can specify your bw

        try {
            // Here we set the bandwidth to be 15MB/s
            HarddriveStorage s1 = new HarddriveStorage(name, 1e12);
            s1.setMaxTransferRate(maxTransferRate);
            storageList.add(s1);
            datacenter = new WorkflowDatacenter(name, characteristics, new VmAllocationPolicySimple(hostList), storageList, 0);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return datacenter;
    }

    /**
     * Prints the total cost
     *
     * @param list list of jobs
     */
    protected static void printTotalCost(List<Job> list) {
    	Log.setOutput(fos);
    	Collections.sort(list, new JobComparator());
        String indent = ",";
    	
        Log.printLine();
        Log.printLine("========== Total Cost ==========");
        double successCost = 0.0;
        double failCost = 0.0;
        
         DecimalFormat dft = new DecimalFormat("###.##");
        for (Job job : list) {
            
            if (job.getCloudletStatus() == Cloudlet.SUCCESS) {
            	 CrowdTask crowdTask = (CrowdTask)job;
            	 successCost += crowdTask.getV();
            } else if (job.getCloudletStatus() == Cloudlet.FAILED) {
            	 CrowdTask crowdTask = (CrowdTask)job;
            	 failCost += crowdTask.getV();
            }
          
            
        }
        Log.printLine("successCost:"+indent+successCost+"failCost:"+indent+failCost);
        Log.setOutput(null);
    }
    /**
     * Prints the job objects
     *
     * @param list list of jobs
     */
    protected static void printJobList(List<Job> list) {
    	Log.setOutput(null);
    	Collections.sort(list, new JobComparator());
        String indent = ",";
    	
        Log.printLine();
        Log.printLine("========== OUTPUT ==========");
       
        Log.printLine("Task ID" + indent + "STATUS" + indent
                + "Data center ID" + indent + "VM ID" + indent 
                + "Time" + indent + "Start Time" + indent + "Finish Time" + indent + "Depth"+ indent + "TaskSolutionType"+ indent + "Trust"+ indent + "Reward");
        DecimalFormat dft = new DecimalFormat("###.##");
        for (Job job : list) {
            Log.print( job.getCloudletId());
            if (job.getClassType() == ClassType.STAGE_IN.value) {
                Log.print("(Stage-in)");
            }
			/*
			 * for (Task task : job.getTaskList()) { CrowdTask crowdTask = (CrowdTask)task;
			 * 
			 * Log.print(task.getCloudletId() + "/" + crowdTask.getTaskSolutionType()); }
			 */
            Log.print(indent);

            if (job.getCloudletStatus() == Cloudlet.SUCCESS) {
                Log.print("SUCCESS");
                Log.print(indent + job.getResourceId() + indent + job.getVmId()
                        + indent + dft.format(job.getActualCPUTime())
                        + indent  + dft.format(job.getExecStartTime())  + indent
                        + dft.format(job.getFinishTime()) + indent  + job.getDepth());
            } else if (job.getCloudletStatus() == Cloudlet.FAILED) {
                Log.print("FAILED");
                Log.print(indent + job.getResourceId()  + indent + job.getVmId()
                         + indent + dft.format(job.getActualCPUTime())
                         + indent + dft.format(job.getExecStartTime())  + indent
                        + dft.format(job.getFinishTime())  + indent + job.getDepth());
            }
            Log.print(indent);
            
           CrowdTask crowdTask = (CrowdTask)job;
            	
           Log.printLine(crowdTask.getTaskSolutionType()+ indent + dft.format(crowdTask.getTrust())   + indent + dft.format(crowdTask.getReward()));
            
        }
        Log.setOutput(null);
    }
    
    /**
     * Prints the job objects
     *
     * @param list list of jobs
     */
    protected static void printVmList(List<CondorVM> condorVMs) {
    	Log.setOutput(fos);
    	List<Worker> workers = new ArrayList<Worker>();
    	for(CondorVM condorVm: condorVMs) {
    		if(condorVm instanceof Worker) {
    			Worker worker = (Worker)condorVm;
    			worker.setGroupNo(0);
    			workers.add(worker);
    		}
    	}
    	Map<Integer, List<Worker>> groupWorkers = groupWorkers(workers);
    	double[] unfairness = calulateUnfairness(groupWorkers);
    	int i =0;
    	 Log.printLine();
         Log.printLine("========== Fairness ==========");
    	for(Integer groupNo: groupWorkers.keySet()) {
    		//for(Worker worker: groupWorkers.get(groupNo)) {
				/*
				 * Log.printLine("groupNo:"+groupNo+","+"worker:"+worker.getId()+","+worker.
				 * getN1()+"," +worker.getN2());
				 */
    			Log.printLine("groupNo:"+groupNo+","+unfairness[i]);
    			i++;
    		//}
    	}
    	Log.printLine();
        Log.printLine("========== Worker ==========");
    	for(Integer groupNo: groupWorkers.keySet()) {
    		for(Worker worker: groupWorkers.get(groupNo)) {
				
				  Log.printLine("groupNo:"+groupNo+","+"worker:"+worker.getId()+","+worker.
				  getN1()+"," +worker.getN2());
				 
    			//Log.printLine("groupNo:"+groupNo+","+unfairness[i]);
    			//i++;
    		}
    	}
    	
    	Log.setOutput(null);
    }
    private static Map<Integer, List<Worker>> groupWorkers(List<Worker> workers){
    	Map<Integer, List<Worker>> groups = new HashMap<Integer,List<Worker>>();
		/*
		 * for(int i=0; i<workers.size(); i++) { Integer groupNo = new Integer(i+1);
		 * List<Worker> gworkers = new ArrayList<Worker>(); Worker currentWorker =
		 * workers.get(i);
		 * 
		 * 
		 * if(currentWorker.getGroupNo()!=0) { continue; }else { for(int j=i+1;
		 * j<workers.size();j++) { boolean flag =
		 * SimilarityUtil.caculateTwoWorkersSimilarity(currentWorker, workers.get(j),
		 * 0.6); if(flag && (workers.get(j).getGroupNo()==0)) {
		 * workers.get(j).setGroupNo(groupNo); gworkers.add(workers.get(j)); } }
		 * 
		 * } currentWorker.setGroupNo(groupNo); gworkers.add(currentWorker);
		 * groups.put(groupNo, gworkers); }
		 */
    	groups.put(0,workers);
		return groups;
	}
    private static double[] calulateUnfairness(Map<Integer, List<Worker>> groupCandidates) {
    	double fairness[] = new double[groupCandidates.keySet().size()];
		int i = 0;
		for(Integer groupNo: groupCandidates.keySet()) {
			List<Worker> groupCandidate = groupCandidates.get(groupNo);
			double sum = 0;
			for(Worker worker: groupCandidate) {
				sum += worker.getN1()/worker.getN2(); 
			}
			double mean = sum/groupCandidate.size();
			
			double sum1 = 0;
			for(Worker worker: groupCandidate) {
				sum1 += Math.pow((mean-worker.getN1()/worker.getN2()), 2); 
			}
			if(groupCandidate.size()==1) {
				fairness[i++] = sum;
			}else {
				fairness[i++] = Math.sqrt(sum1/(groupCandidate.size()-1));
			}
		}
		return fairness;
    }
}

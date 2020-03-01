package org.workflowsim.crowdsourcing.failure;

import org.cloudbus.cloudsim.Cloudlet;
import org.workflowsim.CondorVM;
import org.workflowsim.Job;
import org.workflowsim.Task;
import org.workflowsim.crowdsourcing.Machine;
import org.workflowsim.crowdsourcing.Worker;
import org.workflowsim.crowdsourcing.util.RandomUtil;
import org.workflowsim.failure.FailureGenerator;
import org.workflowsim.failure.FailureMonitor;
import org.workflowsim.failure.FailureParameters;
import org.workflowsim.failure.FailureRecord;

public class CrowdFailureGenerator extends FailureGenerator{

	/**
     * Generates a failure or not
     *
     * @param job
     * @return whether it fails
     */
    //true means has failure
    //false means no failure
	
    public static boolean generate(Job job,CondorVM vm) {
        boolean jobFailed = false;
        if (FailureParameters.getFailureGeneratorMode() == FailureParameters.FTCFailure.FAILURE_NONE) {
            return jobFailed;
        }
        try {

            for (Task task : job.getTaskList()) {
                int failedTaskSum = 0;
               
                if (checkFailureStatus(task, vm)) {
                    //this task fail
                    jobFailed = true;
                    failedTaskSum++;
                    task.setCloudletStatus(Cloudlet.FAILED);
                }
                FailureRecord record = new FailureRecord(0, failedTaskSum, task.getDepth(), 1, job.getVmId(), task.getCloudletId(), job.getUserId());
                FailureMonitor.postFailureRecord(record);
            }

            if (jobFailed) {
                job.setCloudletStatus(Cloudlet.FAILED);
            } else {
                job.setCloudletStatus(Cloudlet.SUCCESS);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return jobFailed;
    }
	protected static boolean checkFailureStatus(Task task,CondorVM vm) throws Exception {
		int completedTaskNumber = 0;
			int allocatedTaskNumber = 0;
			double p0 = 0.0;//(double)completedTaskNumber / (double)allocatedTaskNumber;
			double p1 = 0.0;// machine.getConfigurationMatchProbability();
			double score = 0.0;//w0 * p0 + w1 * p1;
			double w0 =0.5;
			double w1 = 0.5;
       if(vm instanceof Worker) {
    	   Worker worker = (Worker)vm;
    	     completedTaskNumber = worker.getCompletedTaskNumber();
  			 allocatedTaskNumber = worker.getAllocatedTaskNumber();
  			 p0 = (double)completedTaskNumber / (double)allocatedTaskNumber;
  			 p1 = worker.getSkillMatchProbability();
  			 score = w0 * p0 + w1 * p1;
    	   
       }else if(vm instanceof Machine) {
    	  Machine machine = (Machine)vm;
    	  completedTaskNumber = machine.getCompletedTaskNumber();
   		  allocatedTaskNumber = machine.getAllocatedTaskNumber();
   		  p0 = (double)completedTaskNumber / (double)allocatedTaskNumber;
   		  p1 = machine.getConfigurationMatchProbability();
   		  score = w0 * p0 + w1 * p1;
       }
        
        int ar = RandomUtil.randInt(1, 100);
		if(ar>0 && ar<score*100) {
			return false;
		}

        return true;
    }
}

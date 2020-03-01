package org.workflowsim.crowdsourcing;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.workflowsim.FileItem;
import org.workflowsim.Job;
import org.workflowsim.Task;
import org.workflowsim.clustering.BasicClustering;
import org.workflowsim.utils.Parameters.ClassType;
import org.workflowsim.utils.Parameters.FileType;

public class CrowdBasicClustering extends BasicClustering{

	/**
     * Initialize a CrowdBasicClustering object
     */
    public CrowdBasicClustering() {
        super();
    }
    /**
     * The main function of CrowdBasicClustering
     */
    @Override
    public void run() {
        getTask2Job().clear();
        for (Task task : getTaskList()) {
            List<Task> list = new ArrayList<>();
            list.add(task);
            Job job = addTasks2CrowdTask(list);
            job.setVmId(task.getVmId());
            getTask2Job().put(task, job);
        }
        /**
         * Handle the dependencies issue.
         */
        updateDependencies();

    }
    
    /**
     * Add a list of task to a new tasks
     *
     * @param taskList the task list
     * @return tasks the newly created tasks
     */
  
    private  CrowdTask addTasks2CrowdTask(List<Task> taskList) {
        if (taskList != null && !taskList.isEmpty()) {
            int length = 0;

            int userId = 0;
            int priority = 0;
            int depth = 0;
             int taskSolutionType = 0;//任务的求解类型 0:machine 1:human
        	 int taskBusinessType =0;//任务的求解类型 0:网页开发  1:编写函数
        	 List<String> conditions =null;// 求解任务要求的机器配置或工人技能集合
            /// a bug of cloudsim makes it final of input file size and output file size
            CrowdTask job = new CrowdTask(idIndex, length/*, inputFileSize, outputFileSize*/);
            job.setClassType(ClassType.COMPUTE.value);
            for (Task task : taskList) {
                length += task.getCloudletLength();

                userId = task.getUserId();
                priority = task.getPriority();
                depth = task.getDepth();
                taskSolutionType = ((CrowdTask)task).getTaskSolutionType();
                taskBusinessType = ((CrowdTask)task).getTaskBusinessType();
                List<FileItem> fileList = task.getFileList();
                job.getTaskList().add(task);

                getTask2Job().put(task, job);
                for (FileItem file : fileList) {
                    boolean hasFile = job.getFileList().contains(file);
                    if (!hasFile) {
                        job.getFileList().add(file);
                        if (file.getType() == FileType.INPUT) {
                            //for stag-in jobs to be used
                            if (!this.allFileList.contains(file)) {
                                this.allFileList.add(file);
                            }
                        } else if (file.getType() == FileType.OUTPUT) {
                            this.allFileList.add(file);
                        }
                    }
                }
                for (String fileName : task.getRequiredFiles()) {
                    if (!job.getRequiredFiles().contains(fileName)) {
                        job.getRequiredFiles().add(fileName);
                    }
                }
            }

            job.setCloudletLength(length);
            job.setUserId(userId);
            job.setDepth(depth);
            job.setPriority(priority);
            //TODO: 待修改的代码 为啥加上后出错
            job.setTaskBusinessType(taskBusinessType);
            job.setTaskSolutionType(taskSolutionType);
            idIndex++;
            getJobList().add(job);
            return job;
        }

        return null;
    }

}

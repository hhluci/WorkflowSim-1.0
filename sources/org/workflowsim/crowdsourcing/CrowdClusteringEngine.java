package org.workflowsim.crowdsourcing;

import java.util.ArrayList;
import java.util.List;

import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEvent;
import org.workflowsim.ClusteringEngine;
import org.workflowsim.FileItem;
import org.workflowsim.Job;
import org.workflowsim.WorkflowEngine;
import org.workflowsim.WorkflowSimTags;
import org.workflowsim.clustering.BasicClustering;
import org.workflowsim.clustering.BlockClustering;
import org.workflowsim.clustering.HorizontalClustering;
import org.workflowsim.clustering.VerticalClustering;
import org.workflowsim.clustering.balancing.BalancedClustering;
import org.workflowsim.utils.ClusteringParameters;
import org.workflowsim.utils.Parameters;
import org.workflowsim.utils.ReplicaCatalog;
import org.workflowsim.utils.Parameters.ClassType;

public class CrowdClusteringEngine extends ClusteringEngine{

	public CrowdClusteringEngine(String name, int schedulers) throws Exception {
		super(name, schedulers);
		this.workflowEngine = new CrowdWorkflowEngine(name + "_Engine_0", schedulers);
        this.workflowEngineId = this.workflowEngine.getId();
	}
	/**
     * Processes events available for this ClusteringEngine.
     *
     * @pre ev != null
     * @post $none
     */
	@Override
    protected void processClustering() {

        /**
         * The parameters from configuration file
         */
        ClusteringParameters params = Parameters.getClusteringParameters();
        switch (params.getClusteringMethod()) {
            /**
             * Perform Horizontal Clustering
             */
            case HORIZONTAL:
                // if clusters.num is set in configuration file
                if (params.getClustersNum() != 0) {
                    this.engine = new HorizontalClustering(params.getClustersNum(), 0);
                } // if clusters.size is set in configuration file
                else if (params.getClustersSize() != 0) {
                    this.engine = new HorizontalClustering(0, params.getClustersSize());
                }
                break;
            /**
             * Perform Vertical Clustering
             */
            case VERTICAL:
                int depth = 1;
                this.engine = new VerticalClustering(depth);
                break;
            /**
             * Perform Block Clustering
             */
            case BLOCK:
                this.engine = new BlockClustering(params.getClustersNum(), params.getClustersSize());
                break;
            /**
             * Perform Balanced Clustering
             */
            case BALANCED:
                this.engine = new BalancedClustering(params.getClustersNum());
                break;
            /**
             * By default, it does no clustering
             */
            default:
                this.engine = new CrowdBasicClustering();
                break;
        }
        engine.setTaskList(getTaskList());
        engine.run();
        setJobList(engine.getJobList());
    }
	
	/**
     * Adds data stage-in jobs to the job list
     */
	@Override
    protected void processDatastaging() {

        /**
         * All the files of this workflow, it is saved in the workflow engine
         */
        List<FileItem> list = this.engine.getTaskFiles();
        /**
         * A bug of cloudsim, you cannot set the length of a cloudlet to be
         * smaller than 110 otherwise it will fail The reason why we set the id
         * of this job to be getJobList().size() is so that the job id is the
         * next available id
         */
        CrowdTask job = new CrowdTask(getJobList().size(), 110);

        /**
         * This is a very simple implementation of stage-in job, in which we Add
         * all the files to be the input of this stage-in job so that
         * WorkflowSim will transfers them when this job is executed
         */
        List<FileItem> fileList = new ArrayList<>();
        for (FileItem file : list) {
            /**
             * To avoid duplicate files
             */
            if (file.isRealInputFile(list)) {
                ReplicaCatalog.addFileToStorage(file.getName(), Parameters.SOURCE);
                fileList.add(file);
            }
        }
        job.setFileList(fileList);
        job.setClassType(ClassType.STAGE_IN.value);

        /**
         * stage-in is always first level job
         */
        job.setDepth(0);
        job.setPriority(0);

        /**
         * A very simple strategy if you have multiple schedulers and
         * sub-workflows just use the first scheduler
         */
        job.setUserId(getWorkflowEngine().getSchedulerId(0));

        /**
         * add stage-in job
         */
        for (Job cJob : getJobList()) {
            /**
             * first level jobs
             */
            if (cJob.getParentList().isEmpty()) {
                cJob.addParent(job);
                job.addChild(cJob);
            }
        }
        getJobList().add(job);
    }
	 /**
     * Processes events available for this Broker.
     *
     * @param ev a SimEvent object
     * @pre ev != null
     * @post $none
     */
    @Override
    public void processEvent(SimEvent ev) {

        switch (ev.getTag()) {
            case WorkflowSimTags.START_SIMULATION:
                break;
            case WorkflowSimTags.JOB_SUBMIT:
                List list = (List) ev.getData();
                setTaskList(list);
                /**
                 * It doesn't mean we must do clustering here because by default
                 * the processClustering() does nothing unless in the
                 * configuration file we have specified to use clustering
                 */
                processClustering();
                /**
                 * Add stage-in jobs Currently we just add a job that has
                 * minimum runtime but inputs all input data at the beginning of
                 * the workflow execution
                 */
                processDatastaging();
                sendNow(this.workflowEngineId, WorkflowSimTags.JOB_SUBMIT, getJobList());
                break;
            case CloudSimTags.END_OF_SIMULATION:
                shutdownEntity();
                break;
            default:
                processOtherEvent(ev);
                break;
        }
    }
}

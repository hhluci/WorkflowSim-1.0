package org.workflowsim.crowdsourcing;

import java.util.ArrayList;

import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEvent;
import org.workflowsim.WorkflowPlanner;
import org.workflowsim.WorkflowSimTags;

public class CrowdWorkflowPlanner extends WorkflowPlanner{

	private CrowdWorkflowParser crowdWorkflowParser;
	/**
     * Created a new CrowdWorkflowPlanner object.
     *
     * @param name name to be associated with this entity (as required by
     * Sim_entity class from simjava package)
     * @throws Exception the exception
     * @pre name != null
     * @post $none
     */
    public CrowdWorkflowPlanner(String name) throws Exception {
        this(name,1);
        
    }

    public CrowdWorkflowPlanner(String name, int schedulers) throws Exception {
        super(name,schedulers);
        resetClusteringEngine(name,schedulers);

    }
    private void resetClusteringEngine(String name, int schedulers)throws Exception {
    	 setTaskList(new ArrayList<>());
    	this.clusteringEngine = new CrowdClusteringEngine(name + "_Merger_", schedulers);
        this.clusteringEngineId = this.clusteringEngine.getId();
       
        this.crowdWorkflowParser = new CrowdWorkflowParser(getClusteringEngine().getWorkflowEngine().getSchedulerId(0));

    }
    @Override
    public void processEvent(SimEvent ev) {
        switch (ev.getTag()) {
            case WorkflowSimTags.START_SIMULATION:
                //getWorkflowParser().parse();
                //setTaskList(getWorkflowParser().getTaskList());
            	getCrowdWorkflowParser().parse();
            	setTaskList(getCrowdWorkflowParser().getTaskList());
                //processPlanning();
                //processImpactFactors(getTaskList());
                sendNow(getClusteringEngineId(), WorkflowSimTags.JOB_SUBMIT, getTaskList());
                break;
            case CloudSimTags.END_OF_SIMULATION:
                shutdownEntity();
                break;
            // other unknown tags are processed by this method
            default:
                processOtherEvent(ev);
                break;
        }
    }

	public CrowdWorkflowParser getCrowdWorkflowParser() {
		return crowdWorkflowParser;
	}

	public void setCrowdWorkflowParser(CrowdWorkflowParser crowdWorkflowParser) {
		this.crowdWorkflowParser = crowdWorkflowParser;
	}
    

}

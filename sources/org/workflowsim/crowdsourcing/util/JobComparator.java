package org.workflowsim.crowdsourcing.util;

import java.util.Comparator;

import org.workflowsim.Job;

public class JobComparator  implements Comparator<Job>{

	@Override
	public int compare(Job o1, Job o2) {
		// TODO Auto-generated method stub
		return o1.getCloudletId()-o2.getCloudletId();
	}
	

}

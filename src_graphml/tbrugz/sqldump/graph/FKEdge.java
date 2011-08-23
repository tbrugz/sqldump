package tbrugz.sqldump.graph;

import tbrugz.graphml.model.Edge;

public class FKEdge extends Edge {
	public boolean referencesPK;
	
	@Override
	public String getStereotype() {
		return referencesPK?"pkref":"";
	}
	
}

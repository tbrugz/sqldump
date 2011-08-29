package tbrugz.sqldump.graph;

import tbrugz.graphml.model.Edge;

public class FKEdge extends Edge {
	public boolean referencesPK;
	public boolean composite;
	
	@Override
	public String getStereotype() {
		return referencesPK?
				(composite?"composite.pkref":"pkref")
				:
				(composite?"composite.ukref":"ukref");
	}
	
}

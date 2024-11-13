package tbrugz.sqldump.graph;

import tbrugz.graphml.model.Edge;

public class FKEdge extends Edge {
	public Boolean referencesPK;
	public boolean composite;
	
	@Override
	public String getStereotype() {
		return referencesPK==null?
				(composite?"composite.?ref":"?ref"):
			referencesPK?
				(composite?"composite.pkref":"pkref")
				:
				(composite?"composite.ukref":"ukref");
	}
	
}

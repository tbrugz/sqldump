package tbrugz.sqldump.graph;

import tbrugz.graphml.DumpGraphMLModel;
import tbrugz.graphml.model.Edge;

public class DumpResultSetGraphMLModel extends DumpGraphMLModel {

	@Override
	public void outEdgeContents(Edge l, int level) {
		WeightedEdge we = (WeightedEdge) l;
		if(we.getWidth()!=null) {
			outSnippet(getSnippetId(we, "edge"), level, we.getName(), ""+we.getWidth());
		}
		else {
			outSnippet(getSnippetId(we, "edge.nolabel"), level, we.getName(), ""+1);
		}
	}
}

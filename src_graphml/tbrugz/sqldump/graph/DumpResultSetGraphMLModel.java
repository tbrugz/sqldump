package tbrugz.sqldump.graph;

import tbrugz.graphml.DumpGraphMLModel;
import tbrugz.graphml.model.Edge;

public class DumpResultSetGraphMLModel extends DumpGraphMLModel {

	@Override
	public void outEdgeContents(Edge l, int level) {
		WeightedEdge we = (WeightedEdge) l;
		
		String snippetId = null;
		snippetId = getSnippetId(we, "edge");
		
		if(we.getWidth()!=null) {
			outSnippet(snippetId, level, we.getName(), ""+we.getWidth());
		}
		else {
			outSnippet(snippetId, level, we.getName(), ""+1);
		}
	}
}

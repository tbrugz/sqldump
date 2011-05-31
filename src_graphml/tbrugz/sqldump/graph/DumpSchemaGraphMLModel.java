package tbrugz.sqldump.graph;

import tbrugz.graphml.DumpGraphMLModel;
import tbrugz.graphml.model.Edge;
import tbrugz.graphml.model.Node;

public class DumpSchemaGraphMLModel extends DumpGraphMLModel {

	@Override
	public void outNodeContents(Node t, int level) {
		if(t instanceof TableNode) {
			//System.out.println("tt: "+t.getLabel()+"; "+((TableNode)t).getColumnsDesc());
			outSnippet("node", level, t.getLabel(), ((TableNode)t).getColumnsDesc());
		}
		else {
			System.out.println("Error...");
			//outSnippet("node", level, t.getLabel());
		}
	}
	
	@Override
	public void outEdgeContents(Edge l, int level) {
		outSnippet("edge", level, l.getName());
	}
}

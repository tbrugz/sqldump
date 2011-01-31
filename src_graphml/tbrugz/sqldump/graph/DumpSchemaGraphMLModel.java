package tbrugz.sqldump.graph;

import tbrugz.graphml.DumpGraphMLModel;
import tbrugz.graphml.model.Link;
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
	public void outEdgeContents(Link l, int level) {
		outSnippet("edge", level, l.getNome());
	}
}

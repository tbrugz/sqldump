package tbrugz.sqldump.graph;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.graphml.DumpGraphMLModel;
import tbrugz.graphml.model.Edge;
import tbrugz.graphml.model.Node;

public class DumpSchemaGraphMLModel extends DumpGraphMLModel {

	static Log log = LogFactory.getLog(DumpSchemaGraphMLModel.class);
	
	@Override
	public void outNodeContents(Node t, int level) {
		if(t instanceof TableNode) {
			//System.out.println("tt: "+t.getLabel()+"; "+((TableNode)t).getColumnsDesc());
			outSnippet("node", level, t.getLabel(), ((TableNode)t).getColumnsDesc());
		}
		else {
			log.warn("Unknown node type: "+t);
		}
	}
	
	@Override
	public void outEdgeContents(Edge l, int level) {
		outSnippet("edge", level, l.getName());
	}
}

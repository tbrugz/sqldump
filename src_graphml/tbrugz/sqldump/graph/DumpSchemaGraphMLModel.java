package tbrugz.sqldump.graph;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.graphml.DumpGraphMLModel;
import tbrugz.graphml.model.Edge;
import tbrugz.graphml.model.Node;

public class DumpSchemaGraphMLModel extends DumpGraphMLModel {

	static Log log = LogFactory.getLog(DumpSchemaGraphMLModel.class);
	static boolean nodeHeightByColsNumber = true;
	static String snippetsFile;
	
	static final String DEFAULT_NODE_HEIGHT = "400.0";
	
	{
		if(snippetsFile!=null) {
			loadSnippets(snippetsFile);
			log.info("loaded snippets file: '"+snippetsFile+"'");
		}
	}
	
	@Override
	public void outNodeContents(Node t, int level) {
		if(t instanceof TableNode) {
			TableNode tn = (TableNode) t;
			//log.debug("outNode: "+t.getId()+" sn: "+getSnippetId(t, "node")+", st: "+t.getStereotype());
			outSnippet(getSnippetId(t, "node"), level, 
					t.getLabel(), //label 
					tn.getColumnsDesc(), //contents/columns <y:AttributeLabel>
					tn.getConstraintsDesc(), //constraints <y:MethodLabel/>
					nodeHeightByColsNumber?String.valueOf(50 + (tn.getColumnNumber()*15)):DEFAULT_NODE_HEIGHT //height
					);
		}
		else {
			log.warn("Unknown node type: "+t);
		}
	}
	
	@Override
	public void outEdgeContents(Edge l, int level) {
		outSnippet(getSnippetId(l, "edge"), level, l.getName());
	}
}

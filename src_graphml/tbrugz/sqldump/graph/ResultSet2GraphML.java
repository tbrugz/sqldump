package tbrugz.sqldump.graph;

import java.io.File;
import java.io.PrintStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.graphml.DumpGraphMLModel;
import tbrugz.graphml.model.Edge;
import tbrugz.graphml.model.Node;
import tbrugz.graphml.model.Root;
import tbrugz.graphml.model.Stereotyped;
import tbrugz.sqldump.AbstractSQLProc;
import tbrugz.sqldump.IOUtil;
import tbrugz.sqldump.Utils;

/*
 * XXX: option: 2 queries: one for nodes & other for edges
 */
public class ResultSet2GraphML extends AbstractSQLProc {
	
	static Log log = LogFactory.getLog(ResultSet2GraphML.class);
	
	static final String DEFAULT_SNIPPETS = "graphml-snippets-simple.properties";
	
	static final String COL_SOURCE_TYPE = "SOURCE_TYPE";
	static final String COL_SOURCE = "SOURCE";
	static final String COL_TARGET_TYPE = "TARGET_TYPE";
	static final String COL_TARGET = "TARGET";
	
	File output;
	String snippets;
	
	public Root getGraphMlModel(ResultSet rs) throws SQLException {
		Root graphModel = new Root();
		Set<Node> nodes = new HashSet<Node>();
		Set<Edge> edges = new HashSet<Edge>();
		
		while(rs.next()) {
			String objType = rs.getString(COL_SOURCE_TYPE);
			String object = rs.getString(COL_SOURCE);
			String parentType = rs.getString(COL_TARGET_TYPE);
			String parent = rs.getString(COL_TARGET);
			//TODO: edge weight, edge stereotype
			
			Node node = new Node();
			node.setId(object);
			node.setLabel(object);
			node.setStereotype(normalize(objType));
			nodes.add(node);
			log.debug("node "+object+" of stereotype "+node.getStereotype());

			Node parentNode = new Node();
			parentNode.setId(parent);
			parentNode.setLabel(parent);
			parentNode.setStereotype(normalize(parentType));
			nodes.add(parentNode);

			Edge edge = new Edge();
			edge.setSource(object);
			edge.setTarget(parent);
			edges.add(edge);
			edge.setName("");
		}
		
		for(Node n: nodes) {
			//log.debug("node "+n.getId()+" of stereotype "+n.getStereotype());
			graphModel.getChildren().add(n);
		}
		for(Edge e: edges) {
			graphModel.getChildren().add(e);
		}
		
		return graphModel;
	}
	
	static String normalize(String s) {
		//return s;
		return s.replaceAll(" ", "_");
	}
	
	public void dumpSchema(ResultSet rs) throws Exception {
		log.info("dumping graphML: translating model");
		if(rs==null) {
			log.warn("resultSet is null!");
			return;
		}
		Root r = getGraphMlModel(rs);
		log.info("dumping model...");
		DumpGraphMLModel dg = new DumpGraphMLModel();
		dg.loadSnippets(DEFAULT_SNIPPETS);
		if(snippets!=null) {
			dg.loadSnippets(snippets);
		}
		Utils.prepareDir(output);
		dg.dumpModel(r, new PrintStream(output));
		log.info("graphmlquery written to '"+output+"'");
	}

	/*public void setOutput(File output) {
		this.output = output;
	}*/
	
	static void addStereotype(Stereotyped stereo, String str){
		if(stereo.getStereotype()!=null) {
			stereo.setStereotype(stereo.getStereotype()+"."+str);
		}
		else {
			stereo.setStereotype(str);
		}
	}
	
	static final String PROP_GRAPHMLQUERIES = "sqldump.graphmlqueries";
	
	@Override
	public void process() {
		try {
			processIntern();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	void processIntern() throws Exception {
		String queriesStr = prop.getProperty(PROP_GRAPHMLQUERIES);
		if(queriesStr==null) {
			log.warn("prop '"+PROP_GRAPHMLQUERIES+"' not defined");
			return;
		}
		String[] queriesArr = queriesStr.split(",");
		int i=0;
		for(String qid: queriesArr) {
			qid = qid.trim();
			String sql = prop.getProperty("sqldump.graphmlquery."+qid+".sql");
			if(sql==null) {
				//load from file
				String sqlfile = prop.getProperty("sqldump.graphmlquery."+qid+".sqlfile");
				if(sqlfile!=null) {
					sql = IOUtil.readFromFilename(sqlfile);
				}
			}

			String outputfile = prop.getProperty("sqldump.graphmlquery."+qid+".outputfile");
			
			Statement st = conn.createStatement();
			ResultSet rs = st.executeQuery(sql);

			output = new File(outputfile);
			snippets = prop.getProperty("sqldump.graphmlquery."+qid+".snippetsfile");
			dumpSchema(rs);
			
			//log.info("graphmlquery written to '"+outputfile+"'");
			i++;
		}
		log.info(i+" graphmlqueries executed");
	}

}

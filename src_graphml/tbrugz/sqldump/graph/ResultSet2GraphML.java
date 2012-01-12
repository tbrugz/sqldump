package tbrugz.sqldump.graph;

import java.io.File;
import java.io.PrintStream;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
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

class WeightedEdge extends Edge {
	Double width = 0.0;

	public Double getWidth() {
		return width;
	}

	public void setWidth(Double width) {
		this.width = width;
	}
}

/*
 * XXX: option: 2 queries: one for nodes & other for edges
 *      new query: OBJECT, OBJECT_TYPE, OBJECT_LABEL; remove from current query: SOURCE_TYPE, TARGET_TYPE
 *      'label/title node'?
 * XXX: node id + node label     
 */
public class ResultSet2GraphML extends AbstractSQLProc {
	
	static Log log = LogFactory.getLog(ResultSet2GraphML.class);
	
	static final String DEFAULT_SNIPPETS = "graphml-snippets-rs.properties";
	
	static final String COL_SOURCE_TYPE = "SOURCE_TYPE";
	static final String COL_SOURCE = "SOURCE";
	static final String COL_TARGET_TYPE = "TARGET_TYPE";
	static final String COL_TARGET = "TARGET";
	static final String COL_EDGE_TYPE = "EDGE_TYPE";
	static final String COL_EDGE_WIDTH = "EDGE_WIDTH";
	
	static final String[] ALL_COLS = { COL_SOURCE_TYPE, COL_SOURCE, COL_TARGET_TYPE, COL_TARGET, COL_EDGE_TYPE, COL_EDGE_WIDTH, };
	
	//NumberFormat nf = NumberFormat.getInstance();
	NumberFormat nf = new DecimalFormat(",###.00");
	
	//TODO: add property for 'useAbsolute' 
	static boolean useAbsolute = true;
	
	static double edgeMinWidth = 1.0; 
	static double edgeMaxWidth = 7.0; 
	
	File output;
	String snippets;
	
	public Root getGraphMlModel(ResultSet rs) throws SQLException {
		Root graphModel = new Root();
		Set<Node> nodes = new HashSet<Node>();
		Set<WeightedEdge> edges = new HashSet<WeightedEdge>();
		
		double maxWidth = Double.MIN_VALUE, minWidth = Double.MAX_VALUE;
		
		List<String> allCols = Arrays.asList(ALL_COLS);
		if(!hasAllColumns(rs.getMetaData(), allCols)) { return null; }
		
		while(rs.next()) {
			String objType = rs.getString(COL_SOURCE_TYPE);
			String object = rs.getString(COL_SOURCE);
			String parentType = rs.getString(COL_TARGET_TYPE);
			String parent = rs.getString(COL_TARGET);
			Double edgeWidth = rs.getDouble(COL_EDGE_WIDTH);
			String edgeType = rs.getString(COL_EDGE_TYPE);
			//TODOne: node weight
			
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

			WeightedEdge edge = new WeightedEdge();
			edge.setSource(object);
			edge.setTarget(parent);
			edge.setName(nf.format(edgeWidth));
			edge.setWidth(edgeWidth);
			edge.setStereotype(edgeType);
			edges.add(edge);

			double width = useAbsolute? Math.abs(edgeWidth) : edgeWidth;
			
			if(width>maxWidth) { maxWidth = width; }
			if(width<minWidth) { minWidth = width; }
		}
		
		for(Node n: nodes) {
			//log.debug("node "+n.getId()+" of stereotype "+n.getStereotype());
			graphModel.getChildren().add(n);
		}
		//normalize weight 1.0 -> 7.0
		for(WeightedEdge e: edges) {
			log.debug("edge '"+e+"' ["+e.getName()+"] has stereotype: "+e.getStereotype()+"; oldW: "+e.getWidth()+", newW: "+getNewWidth(e.getWidth(), minWidth, maxWidth));
			//log.info("edge: oldW: "+e.getWidth()+", newW: "+getNewWidth(e.getWidth(), minWidth, maxWidth));
			e.setWidth(getNewWidth(e.getWidth(), minWidth, maxWidth));
			graphModel.getChildren().add(e);
		}
		
		return graphModel;
	}
	
	static boolean hasAllColumns(ResultSetMetaData rsmd, List<String> allCols) throws SQLException {
		//test columns
		int colCount = rsmd.getColumnCount();
		List<String> allRSCols = new ArrayList<String>();
		for(int i=0;i<colCount; i++) {
			allRSCols.add(rsmd.getColumnName(i+1));
		}
		if(!allRSCols.containsAll(allCols)) {
			log.warn("query doesn't contain all required columns [required cols are: "+allCols+"; avaiable cols are: "+allRSCols+"]");
			return false;
		}
		return true;
	}
	
	static double getNewWidth(double oldWidht, double minWidth, double maxWidth) {
		double norm = ((useAbsolute?Math.abs(oldWidht):oldWidht) - minWidth) / (maxWidth - minWidth);
		return norm * (edgeMaxWidth - edgeMinWidth) + edgeMinWidth;
	}
	
	static String normalize(String s) {
		//return s;
		if(s==null) { return s; }
		return s.replaceAll(" ", "_");
	}
	
	public boolean dumpSchema(ResultSet rs) throws Exception {
		log.info("dumping graphML: translating model");
		if(rs==null) {
			log.warn("resultSet is null!");
			return false;
		}
		Root r = getGraphMlModel(rs);
		if(r==null) {
			log.warn("null model: nothing to dump");
			return false;
		}
		log.info("dumping model...");
		DumpGraphMLModel dg = new DumpResultSetGraphMLModel();
		//XXX: add prop for selecting snippets file
		dg.loadSnippets(DEFAULT_SNIPPETS);
		if(snippets!=null) {
			dg.loadSnippets(snippets);
		}
		Utils.prepareDir(output);
		dg.dumpModel(r, new PrintStream(output));
		log.info("graphmlquery written to '"+output+"'");
		return true;
	}

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
			
			log.debug("sql: "+sql);
			Statement st = conn.createStatement();
			ResultSet rs = st.executeQuery(sql);

			output = new File(outputfile);
			snippets = prop.getProperty("sqldump.graphmlquery."+qid+".snippetsfile");
			boolean dumped = dumpSchema(rs);
			
			//log.info("graphmlquery written to '"+outputfile+"'");
			if(dumped) { i++; }
		}
		log.info(i+" [of "+queriesArr.length+"] graphmlqueries dumped");
	}

}

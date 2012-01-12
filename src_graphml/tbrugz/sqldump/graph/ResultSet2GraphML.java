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
 * XXXxx: option: 2 queries: one for nodes & other for edges
 *      new query: OBJECT, OBJECT_TYPE, OBJECT_LABEL; remove from current query: SOURCE_TYPE, TARGET_TYPE
 *      'label/title node'?
 * XXX: *_TYPE columns should be optional
 */
public class ResultSet2GraphML extends AbstractSQLProc {
	
	static Log log = LogFactory.getLog(ResultSet2GraphML.class);
	
	static final String DEFAULT_SNIPPETS = "graphml-snippets-rs.properties";

	//node cols
	static final String COL_OBJECT = "OBJECT";
	static final String COL_OBJECT_TYPE = "OBJECT_TYPE";
	static final String COL_OBJECT_LABEL = "OBJECT_LABEL";
	
	//edge cols
	static final String COL_SOURCE = "SOURCE";
	static final String COL_TARGET = "TARGET";
	static final String COL_EDGE_TYPE = "EDGE_TYPE";
	static final String COL_EDGE_WIDTH = "EDGE_WIDTH";
	
	//edge-only cols
	static final String COL_SOURCE_TYPE = "SOURCE_TYPE";
	static final String COL_TARGET_TYPE = "TARGET_TYPE";
	
	static final String[] NODE_COLS = { COL_OBJECT, COL_OBJECT_TYPE, COL_OBJECT_LABEL };
	static final String[] EDGE_COLS = { COL_SOURCE, COL_TARGET, COL_EDGE_TYPE, COL_EDGE_WIDTH };
	static final String[] EDGEONLY_XTRA_COLS = { COL_SOURCE_TYPE, COL_TARGET_TYPE };
	
	//NumberFormat nf = NumberFormat.getInstance();
	static NumberFormat nf = new DecimalFormat(",###.00");
	
	//TODO: add property for 'useAbsolute'
	static boolean useAbsolute = true;
	//TODO: add property for 'doNotDumpNonConnectedNodes'
	static boolean doNotDumpNonConnectedNodes = true;
	
	//TODO: add property for 'edgeMinWidth' & 'edgeMaxWidth'
	static double edgeMinWidth = 1.0; 
	static double edgeMaxWidth = 7.0; 
	
	File output;
	String snippets;
	
	Root getGraphMlModel(ResultSet rsEdges, ResultSet rsNodes) throws SQLException {
		Root graphModel = new Root();
		Set<Node> nodes = new HashSet<Node>();
		Set<WeightedEdge> edges = new HashSet<WeightedEdge>();
		
		boolean edgeOnlyStrategy = rsNodes==null;
		
		Set<String> nodeSet = new HashSet<String>();
		Set<String> edgeEndSet = new HashSet<String>();
		
		double maxWidth = Double.MIN_VALUE, minWidth = Double.MAX_VALUE;
		
		if(!edgeOnlyStrategy) {
			List<String> allNodeCols = Arrays.asList(NODE_COLS);
			if(!hasAllColumns(rsNodes.getMetaData(), allNodeCols)) { return null; }
			
			while(rsNodes.next()) {
				String object = rsNodes.getString(COL_OBJECT);
				String objectType = rsNodes.getString(COL_OBJECT_TYPE);
				String objectLabel = rsNodes.getString(COL_OBJECT_LABEL);

				Node node = new Node();
				node.setId(object);
				node.setLabel(objectLabel);
				node.setStereotype(normalize(objectType));
				nodes.add(node);
				nodeSet.add(node.getId());
				log.debug("node "+object+" of stereotype "+node.getStereotype());
			}
		}
		
		List<String> allCols = Arrays.asList(EDGE_COLS);
		if(edgeOnlyStrategy) {
			List<String> newAllCols = new ArrayList<String>();
			newAllCols.addAll(allCols); newAllCols.addAll(Arrays.asList(EDGEONLY_XTRA_COLS));
			allCols = newAllCols;
		}
		if(!hasAllColumns(rsEdges.getMetaData(), allCols)) { return null; }
		
		while(rsEdges.next()) {
			String source = rsEdges.getString(COL_SOURCE);
			String target = rsEdges.getString(COL_TARGET);
			Double edgeWidth = rsEdges.getDouble(COL_EDGE_WIDTH);
			String edgeType = rsEdges.getString(COL_EDGE_TYPE);

			if(edgeOnlyStrategy) {
				String sourceType = rsEdges.getString(COL_SOURCE_TYPE);
				String targetType = rsEdges.getString(COL_TARGET_TYPE);
				
				Node sourceNode = new Node();
				sourceNode.setId(source);
				sourceNode.setLabel(source);
				sourceNode.setStereotype(normalize(sourceType));
				nodes.add(sourceNode);
				nodeSet.add(sourceNode.getId());
				log.debug("source node "+source+" of stereotype "+sourceNode.getStereotype());
	
				Node targetNode = new Node();
				targetNode.setId(target);
				targetNode.setLabel(target);
				targetNode.setStereotype(normalize(targetType));
				nodes.add(targetNode);
				nodeSet.add(targetNode.getId());
				log.debug("target node "+target+" of stereotype "+targetNode.getStereotype());
			}
			
			WeightedEdge edge = new WeightedEdge();
			edge.setSource(source);
			edge.setTarget(target);
			edge.setName(nf.format(edgeWidth));
			edge.setWidth(edgeWidth);
			edge.setStereotype(edgeType);
			edges.add(edge);
			edgeEndSet.add(source);
			edgeEndSet.add(target);

			double width = useAbsolute? Math.abs(edgeWidth) : edgeWidth;
			
			if(width>maxWidth) { maxWidth = width; }
			if(width<minWidth) { minWidth = width; }
		}
		
		for(Node n: nodes) {
			if(doNotDumpNonConnectedNodes && !edgeEndSet.contains(n.getId())) { continue; }
			graphModel.getChildren().add(n);
		}
		for(WeightedEdge e: edges) {
			if(!nodeSet.contains(e.getSource())) { log.warn("node source '"+e.getSource()+"' not found"); continue; }
			if(!nodeSet.contains(e.getTarget())) { log.warn("node target '"+e.getSource()+"' not found"); continue; }
			
			log.debug("edge '"+e+"' ["+e.getName()+"] has stereotype: "+e.getStereotype()+"; oldW: "+e.getWidth()+", newW: "+getNewWidth(e.getWidth(), minWidth, maxWidth));
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
		if(s==null) { return s; }
		return s.replaceAll(" ", "_");
	}
	
	boolean dumpSchema(ResultSet rsEdges, ResultSet rsNodes) throws Exception {
		log.info("dumping graphML: translating model");
		if(rsEdges==null) {
			log.warn("resultSet is null!");
			return false;
		}
		Root r = getGraphMlModel(rsEdges, rsNodes);
		if(r==null) {
			log.warn("null model: nothing to dump");
			return false;
		}
		log.info("dumping model...");
		DumpGraphMLModel dg = new DumpResultSetGraphMLModel();
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
			
			//edges query
			String sqlEdges = prop.getProperty("sqldump.graphmlquery."+qid+".sql");
			if(sqlEdges==null) {
				//load from file
				String sqlfile = prop.getProperty("sqldump.graphmlquery."+qid+".sqlfile");
				if(sqlfile!=null) {
					sqlEdges = IOUtil.readFromFilename(sqlfile);
				}
			}

			//nodes optional query
			String sqlNodes = prop.getProperty("sqldump.graphmlquery."+qid+".nodesql");
			if(sqlNodes==null) {
				//load from file
				String sqlfile = prop.getProperty("sqldump.graphmlquery."+qid+".nodesqlfile");
				if(sqlfile!=null) {
					sqlNodes = IOUtil.readFromFilename(sqlfile);
				}
			}
			
			String outputfile = prop.getProperty("sqldump.graphmlquery."+qid+".outputfile");
			
			log.debug("edges sql: "+sqlEdges);
			Statement st = conn.createStatement();
			ResultSet rsEdges = st.executeQuery(sqlEdges);
			ResultSet rsNodes = null;

			if(sqlNodes!=null) {
				log.debug("nodes sql: "+sqlNodes);
				st = conn.createStatement();
				rsNodes = st.executeQuery(sqlNodes);
			}
			
			output = new File(outputfile);
			snippets = prop.getProperty("sqldump.graphmlquery."+qid+".snippetsfile");
			boolean dumped = dumpSchema(rsEdges, rsNodes);
			
			if(dumped) { i++; }
		}
		log.info(i+" [of "+queriesArr.length+"] graphmlqueries dumped");
	}

}

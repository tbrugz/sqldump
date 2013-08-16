package tbrugz.sqldump.graph;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
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

import tbrugz.graphml.model.Edge;
import tbrugz.graphml.model.Node;
import tbrugz.graphml.model.NodeXYWH;
import tbrugz.graphml.model.Root;
import tbrugz.graphml.model.Stereotyped;
import tbrugz.sqldump.def.AbstractSQLProc;
import tbrugz.sqldump.def.ProcessingException;
import tbrugz.sqldump.util.IOUtil;
import tbrugz.sqldump.util.ParametrizedProperties;
import tbrugz.sqldump.util.Utils;
import tbrugz.xml.AbstractDump;

class RSNode extends NodeXYWH {
	String description;

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}
	
	@Override
	public String getStereotypeParam(int i) {
		switch (i) {
			case 5:
				return description;
		}
		return super.getStereotypeParam(i);
	}
	
	@Override
	public int getStereotypeParamCount() {
		return 6;
	}
}

class WeightedEdge extends Edge {
	Double width;

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
 * XXXdone: *_TYPE columns should be optional
 * XXXdone: add optional OBJECT_WIDTH, OBJECT_HEIGHT
 */
public class ResultSet2GraphML extends AbstractSQLProc {
	
	static Log log = LogFactory.getLog(ResultSet2GraphML.class);
	static Log logsql = LogFactory.getLog(ResultSet2GraphML.class.getName()+".sql");
	
	static final String DEFAULT_SNIPPETS = "graphml-snippets-rs.properties";
	static final Class<?> DEFAULT_DUMPFORMAT_CLASS = DumpResultSetGraphMLModel.class;
	
	static final String PREFIX_RS2GRAPH = "sqldump.graphmlquery";

	//node cols
	static final String COL_OBJECT = "OBJECT";
	static final String COL_OBJECT_LABEL = "OBJECT_LABEL";
	static final String COL_OBJECT_TYPE = "OBJECT_TYPE"; //optional
	static final String COL_OBJECT_X = "OBJECT_X"; //optional
	static final String COL_OBJECT_Y = "OBJECT_Y"; //optional
	static final String COL_OBJECT_WIDTH = "OBJECT_WIDTH"; //optional
	static final String COL_OBJECT_HEIGHT = "OBJECT_HEIGHT"; //optional
	static final String COL_OBJECT_DESCRIPTION = "OBJECT_DESC"; //optional
	
	//edge cols
	static final String COL_SOURCE = "SOURCE";
	static final String COL_TARGET = "TARGET";
	static final String COL_EDGE_TYPE = "EDGE_TYPE"; //optional
	static final String COL_EDGE_WIDTH = "EDGE_WIDTH"; //optional
	static final String COL_EDGE_LABEL = "EDGE_LABEL"; //optional
	
	//edge-only cols
	static final String COL_SOURCE_TYPE = "SOURCE_TYPE"; //optional
	static final String COL_TARGET_TYPE = "TARGET_TYPE"; //optional
	
	//required cols for node, edge
	static final String[] NODE_COLS = { COL_OBJECT, COL_OBJECT_LABEL };
	static final String[] EDGE_COLS = { COL_SOURCE, COL_TARGET };
	
	//NumberFormat nf = NumberFormat.getInstance();
	static NumberFormat nf = new DecimalFormat(",###.00");
	
	//TODO: add property for 'useAbsolute'
	static boolean useAbsolute = true;
	//TODO: add property for 'doNotDumpNonConnectedNodes'
	static boolean doNotDumpNonConnectedNodes = true;
	
	//TODO: add property for 'edgeMinWidth' & 'edgeMaxWidth'
	static double edgeMinWidth = 0.1; //XXX: maybe 0?
	static double edgeMaxWidth = 7.0; 
	
	Class<?> dumpFormatClass = null;
	File output;
	String snippets;
	
	Root getGraphMlModel(String qid, ResultSet rsEdges, ResultSet rsNodes) throws SQLException {
		Root graphModel = new Root();
		Set<Node> nodes = new HashSet<Node>();
		List<WeightedEdge> edges = new ArrayList<WeightedEdge>();
		
		boolean edgeOnlyStrategy = isEdgeOnlyStrategy(rsEdges, rsNodes);
		
		Set<String> nodeSet = new HashSet<String>();
		Set<String> edgeSourceSet = new HashSet<String>();
		Set<String> edgeTargetSet = new HashSet<String>();
		
		double maxWidth = Double.MIN_VALUE, minWidth = Double.MAX_VALUE;
		
		if(!edgeOnlyStrategy) {
			List<String> allNodeCols = Arrays.asList(NODE_COLS);
			if(!hasAllColumns(rsNodes.getMetaData(), allNodeCols, qid, "nodesql")) { return null; }
			
			boolean hasObjectX = hasOptionalColumn(rsNodes.getMetaData(), COL_OBJECT_X, qid);
			boolean hasObjectY = hasOptionalColumn(rsNodes.getMetaData(), COL_OBJECT_Y, qid);
			boolean hasObjectWidth = hasOptionalColumn(rsNodes.getMetaData(), COL_OBJECT_WIDTH, qid);
			boolean hasObjectHeight = hasOptionalColumn(rsNodes.getMetaData(), COL_OBJECT_HEIGHT, qid);
			
			boolean hasObjectType = hasOptionalColumn(rsNodes.getMetaData(), COL_OBJECT_TYPE, qid);
			boolean hasObjectDesc = hasOptionalColumn(rsNodes.getMetaData(), COL_OBJECT_DESCRIPTION, qid);
			
			while(rsNodes.next()) {
				String object = rsNodes.getString(COL_OBJECT);
				String objectLabel = rsNodes.getString(COL_OBJECT_LABEL);

				RSNode node = newNode();
				node.setId(object);
				node.setLabel(objectLabel);
				if(hasObjectType) {
					String objectType = rsNodes.getString(COL_OBJECT_TYPE);
					node.setStereotype(normalize(objectType));
				}
				if(hasObjectX) {
					node.setX(Float.parseFloat(rsNodes.getString(COL_OBJECT_X)));
				}
				if(hasObjectY) {
					node.setY(Float.parseFloat(rsNodes.getString(COL_OBJECT_Y)));
				}
				if(hasObjectWidth) {
					node.setWidth(Float.parseFloat(rsNodes.getString(COL_OBJECT_WIDTH)));
				}
				if(hasObjectHeight) {
					node.setHeight(Float.parseFloat(rsNodes.getString(COL_OBJECT_HEIGHT)));
				}
				if(hasObjectDesc) {
					String desc = rsNodes.getString(COL_OBJECT_DESCRIPTION);
					node.setDescription(desc);
				}
				else {
					node.setDescription("");
				}
				nodes.add(node);
				nodeSet.add(node.getId());
				log.debug("node "+object+" of stereotype "+node.getStereotype());
			}
		}
		
		List<String> allCols = Arrays.asList(EDGE_COLS);
		boolean hasSourceType = false;
		boolean hasTargetType = false;
		boolean hasEdgeWidth = false;
		boolean hasEdgeType = false;
		boolean hasEdgeLabel = false;
		if(edgeOnlyStrategy) {
			//List<String> newAllCols = new ArrayList<String>();
			//newAllCols.addAll(allCols); newAllCols.addAll(Arrays.asList(EDGEONLY_XTRA_COLS));
			//allCols = newAllCols;
			hasSourceType = hasOptionalColumn(rsEdges.getMetaData(), COL_SOURCE_TYPE, qid);
			hasTargetType = hasOptionalColumn(rsEdges.getMetaData(), COL_TARGET_TYPE, qid);
		}
		hasEdgeWidth = hasOptionalColumn(rsEdges.getMetaData(), COL_EDGE_WIDTH, qid);
		hasEdgeType = hasOptionalColumn(rsEdges.getMetaData(), COL_EDGE_TYPE, qid);
		hasEdgeLabel = hasOptionalColumn(rsEdges.getMetaData(), COL_EDGE_LABEL, qid);
		if(!hasAllColumns(rsEdges.getMetaData(), allCols, qid, "edgesql")) { return null; }
		
		while(rsEdges.next()) {
			String source = rsEdges.getString(COL_SOURCE);
			String target = rsEdges.getString(COL_TARGET);
			Double edgeWidth = null;
			String edgeType = "";

			if(edgeOnlyStrategy) {
				Node sourceNode = newNode();
				sourceNode.setId(source);
				sourceNode.setLabel(source);
				if(hasSourceType) {
					String sourceType = rsEdges.getString(COL_SOURCE_TYPE);
					sourceNode.setStereotype(normalize(sourceType));
				}
				
				if(nodeSet.add(sourceNode.getId())) {
					nodes.add(sourceNode);
					log.debug("source node "+source+" of stereotype "+sourceNode.getStereotype());
				}
				else {
					log.debug("source node "+source+" already processed");
				}
	
				Node targetNode = newNode();
				targetNode.setId(target);
				targetNode.setLabel(target);
				if(hasTargetType) {
					String targetType = rsEdges.getString(COL_TARGET_TYPE);
					targetNode.setStereotype(normalize(targetType));
				}
				
				if(nodeSet.add(targetNode.getId())) {
					nodes.add(targetNode);
					log.debug("target node "+target+" of stereotype "+targetNode.getStereotype());
				}
				else {
					log.debug("target node "+target+" already processed");
				}
			}
			
			WeightedEdge edge = new WeightedEdge();
			if(source==null || target==null) {
				log.warn("null source/target: "+source+"/"+target);
				continue;
			}
			if(hasEdgeWidth) {
				edgeWidth = rsEdges.getDouble(COL_EDGE_WIDTH);
				if(!hasEdgeLabel) {
					edge.setName(nf.format(edgeWidth));
				}
				edge.setWidth(edgeWidth);

				double width = useAbsolute? Math.abs(edgeWidth) : edgeWidth;
				if(width>maxWidth) { maxWidth = width; }
				if(width<minWidth) { minWidth = width; }
			}
			if(hasEdgeType) {
				edgeType = rsEdges.getString(COL_EDGE_TYPE);
			}
			if(hasEdgeLabel) {
				edge.setName(rsEdges.getString(COL_EDGE_LABEL) 
					//+ ((edge.getName()!=null)?" / "+edge.getName():"")
					);
			}
			edge.setSource(source);
			edge.setTarget(target);
			edge.setStereotype(edgeType);
			edges.add(edge);
			edgeSourceSet.add(source);
			edgeTargetSet.add(target);
		}
		
		for(Node n: nodes) {
			if(doNotDumpNonConnectedNodes && !edgeSourceSet.contains(n.getId()) && !edgeTargetSet.contains(n.getId())) { continue; }
			//XXX: add source-only or target-only stereotypes?
			//if(!edgeSourceSet.contains(n.getId())) { n.setFinalNode(true); }
			//if(!edgeTargetSet.contains(n.getId())) { n.setInitialNode(true); }
			graphModel.getChildren().add(n);
		}
		for(WeightedEdge e: edges) {
			if(!nodeSet.contains(e.getSource())) { log.warn("node source '"+e.getSource()+"' not found"); continue; }
			if(!nodeSet.contains(e.getTarget())) { log.warn("node target '"+e.getTarget()+"' not found"); continue; }
			
			log.debug("edge '"+e+"' ["+e.getName()+"] has stereotype: "+e.getStereotype()+"; oldW: "+e.getWidth()+", newW: "+getNewWidth(e.getWidth(), minWidth, maxWidth));
			e.setWidth(getNewWidth(e.getWidth(), minWidth, maxWidth));
			graphModel.getChildren().add(e);
		}
		
		return graphModel;
	}
	
	static boolean hasAllColumns(ResultSetMetaData rsmd, List<String> allCols, String queryId, String queryType) throws SQLException {
		//test columns
		int colCount = rsmd.getColumnCount();
		List<String> allRSCols = new ArrayList<String>();
		for(int i=0;i<colCount; i++) {
			allRSCols.add(rsmd.getColumnLabel(i+1));
		}
		if(!allRSCols.containsAll(allCols)) {
			log.warn("query '"+queryId+"' ["+queryType+"] doesn't contain all required columns [required cols are: "+allCols+"; avaiable cols are: "+allRSCols+"]");
			return false;
		}
		return true;
	}
	
	static boolean hasOptionalColumn(ResultSetMetaData rsmd, String column, String qid) throws SQLException {
		boolean hasit = hasColumn(rsmd, column);
		if(!hasit) {
			log.info("doesn't have optional column '"+column+"' [qid = "+qid+"]");
			return false;
		}
		log.info("has optional column '"+column+"' [qid = "+qid+"]");
		return true;
	}

	static boolean hasColumn(ResultSetMetaData rsmd, String column) throws SQLException {
		//test columns
		int colCount = rsmd.getColumnCount();
		List<String> allRSCols = new ArrayList<String>();
		for(int i=0;i<colCount; i++) {
			allRSCols.add(rsmd.getColumnLabel(i+1));
		}
		if(!allRSCols.contains(column)) {
			//log.info("doesn't have optional column '"+column+"'");
			return false;
		}
		//log.info("has optional column '"+column+"'");
		return true;
	}
	
	static Double getNewWidth(Double oldWidth, double minWidth, double maxWidth) {
		if(oldWidth==null) { return null; }
		double norm = ((useAbsolute?Math.abs(oldWidth):oldWidth) - minWidth) / (maxWidth - minWidth);
		return norm * (edgeMaxWidth - edgeMinWidth) + edgeMinWidth;
	}
	
	static String normalize(String s) {
		if(s==null) { return s; }
		return s.replaceAll(" ", "_");
	}
	
	boolean dumpSchema(String qid, ResultSet rsEdges, ResultSet rsNodes) throws SQLException, FileNotFoundException {
		log.info("dumping graphML: translating model [edgeonly="+isEdgeOnlyStrategy(rsEdges, rsNodes)+"]");
		if(rsEdges==null) {
			String message = "resultSet is null!";
			log.warn(message);
			if(failonerror) {
				throw new ProcessingException(message);
			}
			return false;
		}
		Root r = getGraphMlModel(qid, rsEdges, rsNodes);
		if(r==null) {
			String message = "null model: nothing to dump";
			log.warn(message);
			if(failonerror) {
				throw new ProcessingException(message);
			}
			return false;
		}
		log.info("dumping model... [graph size = "+r.getChildren().size()+"]");
		
		AbstractDump dg = (AbstractDump) Utils.getClassInstance(dumpFormatClass);
		dg.loadSnippets(DEFAULT_SNIPPETS);
		if(snippets!=null) {
			try {
				dg.loadSnippets(snippets);
			}
			catch(NullPointerException e) {
				log.warn("error opening snippets file: "+snippets);
			}
		}
		Utils.prepareDir(output);
		dg.dumpModel(r, new PrintStream(output));
		log.info("graphmlquery written to '"+output.getAbsolutePath()+"'");
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
			if(failonerror) {
				throw new ProcessingException(e);
			}
		}
	}
	
	void processIntern() throws SQLException, IOException {
		String queriesStr = prop.getProperty(PROP_GRAPHMLQUERIES);
		if(queriesStr==null) {
			String message = "prop '"+PROP_GRAPHMLQUERIES+"' not defined";
			log.warn(message);
			if(failonerror) {
				throw new ProcessingException(message);
			}
			return;
		}
		String[] queriesArr = queriesStr.split(",");
		int i=0;
		for(String qid: queriesArr) {
			qid = qid.trim();
			String queryPrefix = PREFIX_RS2GRAPH+"."+qid;
			
			//edges query
			String sqlEdges = prop.getProperty(queryPrefix+".sql");
			if(sqlEdges==null) {
				//load from file
				String sqlfile = prop.getProperty(queryPrefix+".sqlfile");
				if(sqlfile!=null) {
					sqlEdges = IOUtil.readFromFilename(sqlfile);
					sqlEdges = ParametrizedProperties.replaceProps(sqlEdges, prop);
				}
			}

			//nodes optional query
			String sqlNodes = prop.getProperty(queryPrefix+".nodesql");
			if(sqlNodes==null) {
				//load from file
				String sqlfile = prop.getProperty(queryPrefix+".nodesqlfile");
				if(sqlfile!=null) {
					sqlNodes = IOUtil.readFromFilename(sqlfile);
					sqlNodes = ParametrizedProperties.replaceProps(sqlNodes, prop);
				}
			}
			
			String propOutFile = queryPrefix+".outputfile";
			String outputfile = prop.getProperty(propOutFile);
			if(outputfile==null) {
				log.error("output file not defined (prop '"+propOutFile+"')");
				return;
			}
			
			logsql.info("edges sql: "+sqlEdges);
			Statement st = conn.createStatement();
			ResultSet rsEdges = st.executeQuery(sqlEdges);
			ResultSet rsNodes = null;

			if(sqlNodes!=null) {
				logsql.info("nodes sql: "+sqlNodes);
				st = conn.createStatement();
				rsNodes = st.executeQuery(sqlNodes);
			}
			
			output = new File(outputfile);
			snippets = prop.getProperty(queryPrefix+".snippetsfile");
			dumpFormatClass = Schema2GraphML.getDumpFormatClass(prop,
					queryPrefix+Schema2GraphML.SUFFIX_DUMPFORMATCLASS);
			if(dumpFormatClass!=null) {
				log.info("dump format class: "+dumpFormatClass.getName()+" [qid = "+qid+"]");
			}
			else {
				dumpFormatClass = DEFAULT_DUMPFORMAT_CLASS;
			}
			
			boolean dumped = dumpSchema(qid, rsEdges, rsNodes);
			
			if(dumped) { i++; }
		}
		log.info(i+" [of "+queriesArr.length+"] graphmlqueries dumped");
	}
	
	static boolean isEdgeOnlyStrategy(ResultSet rsEdges, ResultSet rsNodes) {
		return rsNodes==null;
	}
	
	static RSNode newNode() {
		RSNode node = new RSNode();
		node.setHeight(50f);
		node.setStereotype(null);
		return node;
	}
	
}

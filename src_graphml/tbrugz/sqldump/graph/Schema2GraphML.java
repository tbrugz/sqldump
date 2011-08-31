package tbrugz.sqldump.graph;

import java.io.File;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.graphml.DumpGraphMLModel;
import tbrugz.graphml.model.Edge;
import tbrugz.graphml.model.Root;
import tbrugz.graphml.model.Stereotyped;
import tbrugz.sqldump.SQLDump;
import tbrugz.sqldump.SchemaModel;
import tbrugz.sqldump.SchemaModelDumper;
import tbrugz.sqldump.Utils;
import tbrugz.sqldump.dbmodel.Column;
import tbrugz.sqldump.dbmodel.FK;
import tbrugz.sqldump.dbmodel.Table;

enum EdgeLabelType {
	FK, FKANDCOLUMNS, COLUMNS, NONE;
}

/*
 * XXX: node contents: show PK columns, FK columns, constraints?
 * TODOne: stereotype may include 'otherschema' or table type (table, view, synonym(?), external table)
 * XXX: stereotype may be 'temporary table' 
 * XXX: node color by row count?
 * XXX: stereotypes: FK-source-only nodes, FK-target-only nodes
 * XXXdone: height by col size?
 * XXXdone: prop for toggling on/off the adjustment of node height?
 * TODO: prop for setting 'graphml-snippets.properties' location
 * TODO: merge with existing graphml (then layout is not lost) 
 *  - update node contents, update edge label, add new nodes and edges
 *  - specific stereotype for 'new' nodes & edges
 * XXXdone: FK stereotype: composite?
 * XXXdone: schema_name as node stereotype?
 * XXXdone: table name pattern as node stereotype
 */
public class Schema2GraphML implements SchemaModelDumper {
	
	static Log log = LogFactory.getLog(Schema2GraphML.class);
	
	public static final String PROP_OUTPUTFILE = "sqldump.graphmldump.outputfile";
	public static final String PROP_SHOWSCHEMANAME = "sqldump.graphmldump.showschemaname";
	public static final String PROP_EDGELABEL = "sqldump.graphmldump.edgelabel";
	public static final String PROP_NODEHEIGHTBYCOLSNUMBER = "sqldump.graphmldump.nodeheightbycolsnumber";
	
	public static final String PROP_NODESTEREOTYPEREGEX_PREFIX = "sqldump.graphmldump.nodestereotyperegex.";

	File output;
	List<String> schemaNamesList = new ArrayList<String>();
	EdgeLabelType edgeLabel = EdgeLabelType.NONE;
	boolean showSchemaName = true;
	Map<String, Pattern> stereotypeRegexes = new HashMap<String, Pattern>();
	
	//String defaultSchemaName;
	
	public Root getGraphMlModel(SchemaModel schemaModel) {
		Root graphModel = new Root();
		
		Set<String> tableIds = new HashSet<String>();
		
		for(Table t: schemaModel.getTables()) {
			TableNode n = new TableNode();
			String id = t.getSchemaName()+"."+t.getName();
			n.setId(id);
			if(showSchemaName) {
				n.setLabel(id);
			}
			else {
				n.setLabel(t.getName());
			}
			n.setColumnNumber(t.getColumns().size());
			StringBuffer sb = new StringBuffer();
			//sb.append("--type: "+t.type+"\n");
			for(Column c: t.getColumns()) {
				sb.append(Column.getColumnDescFull(c, null, null, null)+"\n");
			}
			n.setColumnsDesc(sb.toString());
			
			//node stereotype
			switch (t.getType()) {
				case SYSTEM_TABLE: 
				case VIEW: 
					n.setStereotype("type@"+t.getType()); break;
				case TABLE:
				default:
					break;
			}
			for(String key: stereotypeRegexes.keySet()) {
				if(stereotypeRegexes.get(key).matcher(t.getName()).matches()) {
					addStereotype(n, "regex@"+key);
				}
			}
			if(t.schemaName!=null && schemaNamesList.contains(t.schemaName)) {
				addStereotype(n, "schema@"+t.schemaName);
			}
			else {
				addStereotype(n, "otherschema.schema@"+t.schemaName);
			}
			//end stereotype
			
			tableIds.add(id);
			graphModel.getChildren().add(n);
		}
		
		for(FK fk: schemaModel.getForeignKeys()) {
			Edge l = fkToLink(fk);
			if(tableIds.contains(l.getSource())) {
				if(tableIds.contains(l.getTarget())) {
					graphModel.getChildren().add(l);
				}
				else {
					log.warn("Link end ["+l.getTarget()+"] not found");
				}
			}
			else {
				log.warn("Link end ["+l.getSource()+"] not found");
			}
		}
		
		
		return graphModel;
	}
	
	Edge fkToLink(FK fk) {
		FKEdge l = new FKEdge();
		switch(edgeLabel) {
			case FK: 
				l.setName(fk.getName()); break;
			case COLUMNS: 
				l.setName("("+fk.fkColumns+" -> "+fk.pkColumns+")"); break;
			case FKANDCOLUMNS:
				//fk label: fk name + columns involved
				l.setName(fk.getName()+" ("+fk.fkColumns+" -> "+fk.pkColumns+")"); break;
			case NONE:
			default:
				l.setName(""); break;
		}
		l.referencesPK = fk.fkReferencesPK;
		l.composite = fk.fkColumns.size()>1;
		l.setSource(fk.getSourceId());
		l.setTarget(fk.getTargetId());
		
		return l;
	}

	@Override
	public void dumpSchema(SchemaModel schemaModel) throws Exception {
		log.info("dumping graphML: translating model");
		if(schemaModel==null) {
			log.warn("schemaModel is null!");
			return;
		}
		Root r = getGraphMlModel(schemaModel);
		log.info("dumping model...");
		DumpGraphMLModel dg = new DumpSchemaGraphMLModel();
		Utils.prepareDir(output);
		dg.dumpModel(r, new PrintStream(output));
		log.info("...graphML dumped");
	}

	public void setOutput(File output) {
		this.output = output;
	}
	
	@Override
	public void procProperties(Properties prop) {
		String outFileStr = prop.getProperty(PROP_OUTPUTFILE);
		String schemaPattern = prop.getProperty(SQLDump.PROP_DUMPSCHEMAPATTERN);
		
		String[] schemasArr = schemaPattern.split(",");
		schemaNamesList = new ArrayList<String>();
		for(String schemaName: schemasArr) {
			schemaNamesList.add(schemaName.trim());
		}
		
		//sqldump.graphmldump.edgelabel=FK|FKANDCOLUMNS|COLUMNS|NONE
		String edgeLabelStr = prop.getProperty(PROP_EDGELABEL, EdgeLabelType.NONE.name());
		try {
			edgeLabel = EdgeLabelType.valueOf(edgeLabelStr);
		}
		catch(IllegalArgumentException e) {
			log.warn("Illegal value for prop '"+PROP_EDGELABEL+"': "+edgeLabelStr);
		}
		
		//node stereotype regex
		for(String key: Utils.getKeysStartingWith(prop, PROP_NODESTEREOTYPEREGEX_PREFIX)) {
			String stereotype = key.substring(PROP_NODESTEREOTYPEREGEX_PREFIX.length());
			String regex = prop.getProperty(key);
			stereotypeRegexes.put(stereotype, Pattern.compile(regex));
		}
		
		showSchemaName = Utils.getPropBool(prop, PROP_SHOWSCHEMANAME, true);
		DumpSchemaGraphMLModel.nodeHeightByColsNumber = Utils.getPropBool(prop, PROP_NODEHEIGHTBYCOLSNUMBER, true);
		
		setOutput(new File(outFileStr));
	}
	
	
	static void addStereotype(Stereotyped stereo, String str){
		if(stereo.getStereotype()!=null) {
			stereo.setStereotype(stereo.getStereotype()+"."+str);
		}
		else {
			stereo.setStereotype(str);
		}
	}
}

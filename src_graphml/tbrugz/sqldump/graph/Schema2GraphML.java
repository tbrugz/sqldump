package tbrugz.sqldump.graph;

import java.io.File;
import java.io.FileNotFoundException;
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
import tbrugz.sqldump.dbmodel.Constraint;
import tbrugz.sqldump.dbmodel.FK;
import tbrugz.sqldump.dbmodel.Index;
import tbrugz.sqldump.dbmodel.Table;

enum EdgeLabelType {
	FK, FKANDCOLUMNS, COLUMNS, NONE;
}

/*
 * TODO!: node contents: show PK columns, FK columns, IDX columns (at column side) - optional: order cols by name 
 * TODOne: add constraints (PK, UK, (check?,) indexes) inside <y:MethodLabel/>
 * TODOne: stereotype may include 'otherschema' or table type (table, view, synonym(?), external table)
 * XXX: stereotype may be 'temporary table' 
 * XXX: node color by row count?
 * XXXdone: stereotypes: FK-source-only nodes (roots), FK-target-only nodes (leafs)
 * XXXdone: height by col size?
 * XXXdone: prop for toggling on/off the adjustment of node height?
 * TODOne: prop for setting 'graphml-snippets.properties' location
 * TODO: merge with existing graphml (then layout is not lost) 
 *  - update node contents, update edge label, add new nodes and edges
 *  - specific stereotype for 'new' nodes & edges
 * XXXdone: FK stereotype: composite?
 * XXXdone: schema_name as node stereotype?
 * XXXdone: table name pattern as node stereotype
 * XXXdone: option to show only table names - see/use: /sqldump/src_graphml/graphml-snippets-simple.properties
 * XXX: node label: show table type (VIEW, EXTERNAL_TABLE, ...)?
 * XXX: option to map one stereotype to another (schema@XXX -> schema@YYY; type@EXTERNAL_TABLE -> type@VIEW)
 */
public class Schema2GraphML implements SchemaModelDumper {
	
	static Log log = LogFactory.getLog(Schema2GraphML.class);
	
	public static final String PROP_OUTPUTFILE = "sqldump.graphmldump.outputfile";
	public static final String PROP_SHOWSCHEMANAME = "sqldump.graphmldump.showschemaname";
	public static final String PROP_SHOWCONSTRAINTS = "sqldump.graphmldump.showconstraints";
	public static final String PROP_SHOWINDEXES = "sqldump.graphmldump.showindexes";
	public static final String PROP_EDGELABEL = "sqldump.graphmldump.edgelabel";
	public static final String PROP_NODEHEIGHTBYCOLSNUMBER = "sqldump.graphmldump.nodeheightbycolsnumber";
	public static final String PROP_SNIPPETS_FILE = "sqldump.graphmldump.snippetsfile";
	public static final String PROP_STEREOTYPE_ADD_TABLETYPE = "sqldump.graphmldump.addtabletypestereotype";
	public static final String PROP_STEREOTYPE_ADD_SCHEMA = "sqldump.graphmldump.addschemastereotype";
	public static final String PROP_STEREOTYPE_ADD_VERTEXTYPE = "sqldump.graphmldump.addvertextypestereotype";
	
	public static final String PROP_NODESTEREOTYPEREGEX_PREFIX = "sqldump.graphmldump.nodestereotyperegex.";

	File output;
	List<String> schemaNamesList = new ArrayList<String>();
	EdgeLabelType edgeLabel = EdgeLabelType.NONE;
	boolean showSchemaName = true;
	boolean showConstraints = false;
	boolean showIndexes = false;
	
	boolean addTableTypeStereotype = true;
	boolean addSchemaStereotype = false;
	boolean addVertexTypeStereotype = false;
	
	Map<String, List<Pattern>> stereotypeRegexes = new HashMap<String, List<Pattern>>();
	
	//String defaultSchemaName;
	
	public Root getGraphMlModel(SchemaModel schemaModel) {
		Root graphModel = new Root();
		
		Set<String> tableIds = new HashSet<String>();
		
		for(Table t: schemaModel.getTables()) {
			TableNode n = new TableNode();
			String id = t.getSchemaName()+"."+t.getName();
			n.setId(id);
			
			//label
			if(showSchemaName) {
				n.setLabel(id);
			}
			else {
				n.setLabel(t.getName());
			}
			
			//columns
			int colCount = setTableNodeAttributes(n, schemaModel, t);
			/*StringBuffer sbCols = new StringBuffer();
			for(Column c: t.getColumns()) {
				sbCols.append(Column.getColumnDesc(c, null, null, null)+"\n");
			}
			n.setColumnsDesc(sbCols.toString());*/

			//constraints + indexes
			int methodCount = setTableNodeMethods(n, schemaModel, t);
			/*n.setConstraintsDesc("");
			int constrCount = 0;
			if(showConstraints) {
				StringBuffer sbConstraints = new StringBuffer();
				for(Constraint cons: t.getConstraints()) {
					switch(cons.type) {
						case PK: 
						case CHECK:
						case UNIQUE:
							sbConstraints.append(cons.getDefinition(false)+"\n");
							constrCount++;
					}
				}
				n.setConstraintsDesc(n.getConstraintsDesc()+sbConstraints.toString());
			}
			//indexes
			int indexCount = 0;
			if(showIndexes) {
				StringBuffer sbIndexes = new StringBuffer();
				for(Index idx: schemaModel.getIndexes()) {
					//log.debug("idx: "+idx+" / t: "+t.name);
					if(idx.tableName.equals(t.name)) {
						sbIndexes.append(idx.getDefinition(false)+"\n");
						indexCount++;
					}
				}
				n.setConstraintsDesc(n.getConstraintsDesc()+sbIndexes.toString());
			}*/
	
			//root, leaf
			boolean isRoot = true;
			boolean isLeaf = true;
			for(FK fk: schemaModel.getForeignKeys()) {
				if(fk.pkTable.equals(t.getName())) {
					isRoot = false;
				}
				if(fk.fkTable.equals(t.getName())) {
					isLeaf = false;
				}
			}
			n.setRoot(isRoot);
			n.setLeaf(isLeaf);

			//column number
			n.setColumnNumber(colCount+methodCount);
			
			//node stereotype
			if(addTableTypeStereotype) {
				switch (t.getType()) {
					case SYSTEM_TABLE: 
					case VIEW:
					case MATERIALIZED_VIEW:
					case EXTERNAL_TABLE:
					case SYNONYM:
						addStereotype(n, "type@"+t.getType()); break;
					case TABLE:
					default:
						break;
				}
			}
			for(String key: stereotypeRegexes.keySet()) {
				//List<Pattern> patterns = stereotypeRegexes.get(key);
				for(Pattern pat: stereotypeRegexes.get(key)) {
					if(pat.matcher(t.getName()).matches()) {
						addStereotype(n, "regex@"+key);
					}
				}
			}
			
			if(t.schemaName!=null && schemaNamesList.contains(t.schemaName)) {
				if(addSchemaStereotype) {
					addStereotype(n, "schema@"+t.schemaName);
				}
			}
			else {
				if(addSchemaStereotype) {
					addStereotype(n, "otherschema.schema@"+t.schemaName);
				}
				else {
					addStereotype(n, "otherschema");
				}
			}
			
			if(addVertexTypeStereotype) {
				if(n.isRoot() && n.isLeaf()) {
					//log.debug("table '"+t.getName()+"' is disconnected");
					addStereotype(n, "disconnected");
				}
				else if(n.isRoot()) {
					//log.debug("table '"+t.getName()+"' is root");
					addStereotype(n, "root");
				}
				else if(n.isLeaf()) {
					//log.debug("table '"+t.getName()+"' is leaf");
					addStereotype(n, "leaf");
				}
				else {
					//is in the middle
					addStereotype(n, "connected");
				}
			}
			if(n.getStereotype()!=null) {
				log.debug("node '"+t.getName()+"' has stereotype: "+n.getStereotype());
			}
			else {
				log.debug("node '"+t.getName()+"' has no stereotype");
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
	
	int setTableNodeAttributes(TableNode n, SchemaModel schemaModel, Table t) {
		//columns
		StringBuffer sbCols = new StringBuffer();
		int colCount = 0;
		for(Column c: t.getColumns()) {
			sbCols.append(Column.getColumnDesc(c, null, null, null)+"\n");
			colCount++;
		}
		n.setColumnsDesc(sbCols.toString());
		return colCount;
	}

	int setTableNodeMethods(TableNode n, SchemaModel schemaModel, Table t) {
		//constraints
		n.setConstraintsDesc("");
		int constrCount = 0;
		if(showConstraints) {
			StringBuffer sbConstraints = new StringBuffer();
			for(Constraint cons: t.getConstraints()) {
				switch(cons.type) {
					case PK: 
					case CHECK:
					case UNIQUE:
						sbConstraints.append(cons.getDefinition(false)+"\n");
						constrCount++;
				}
			}
			n.setConstraintsDesc(n.getConstraintsDesc()+sbConstraints.toString());
		}
		//indexes
		int indexCount = 0;
		if(showIndexes) {
			StringBuffer sbIndexes = new StringBuffer();
			for(Index idx: schemaModel.getIndexes()) {
				//log.debug("idx: "+idx+" / t: "+t.name);
				if(idx.tableName.equals(t.name)) {
					sbIndexes.append(idx.getDefinition(false)+"\n");
					indexCount++;
				}
			}
			n.setConstraintsDesc(n.getConstraintsDesc()+sbIndexes.toString());
		}
		return constrCount+indexCount;
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
	public void dumpSchema(SchemaModel schemaModel) {
		if(output==null) {
			log.warn("graphml output file is null. won't dump");
			return;
		}
		
		log.info("dumping graphML: translating model");
		if(schemaModel==null) {
			log.warn("schemaModel is null!");
			return;
		}
		Root r = getGraphMlModel(schemaModel);
		log.info("dumping model...");
		DumpGraphMLModel dg = new DumpSchemaGraphMLModel();
		Utils.prepareDir(output);
		
		try {
			dg.dumpModel(r, new PrintStream(output));
			log.info("...graphML dumped");
		} catch (FileNotFoundException e) {
			log.warn("error dumping schema: "+e);
			log.debug("error dumping schema", e);
		}
	}

	public void setOutput(File output) {
		this.output = output;
	}
	
	@Override
	public void procProperties(Properties prop) {
		String outFileStr = prop.getProperty(PROP_OUTPUTFILE);
		if(outFileStr==null) {
			log.warn("graphml output file ["+PROP_OUTPUTFILE+"] not defined");
			return;
		}
		
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
			String[] regexes = prop.getProperty(key).split("\\|");
			for(String regex: regexes) {
				regex = regex.trim();
				log.debug("added stereotype table pattern: stereotype: '"+stereotype+"', pattern: '"+regex+"'");
				List<Pattern> patterns = stereotypeRegexes.get(stereotype);
				if(patterns==null) {
					patterns = new ArrayList<Pattern>();
					stereotypeRegexes.put(stereotype, patterns);
				}
				patterns.add(Pattern.compile(regex));
			}
		}
		
		showSchemaName = Utils.getPropBool(prop, PROP_SHOWSCHEMANAME, showSchemaName);
		showConstraints = Utils.getPropBool(prop, PROP_SHOWCONSTRAINTS, showConstraints);
		showIndexes = Utils.getPropBool(prop, PROP_SHOWINDEXES, showIndexes);
		DumpSchemaGraphMLModel.nodeHeightByColsNumber = Utils.getPropBool(prop, PROP_NODEHEIGHTBYCOLSNUMBER, true);
		
		String snippetsFile = prop.getProperty(PROP_SNIPPETS_FILE);
		if(snippetsFile!=null) {
			DumpSchemaGraphMLModel.snippetsFile = snippetsFile;
			log.debug("snippets-file is '"+DumpSchemaGraphMLModel.snippetsFile+"'");
		}
		
		addTableTypeStereotype = Utils.getPropBool(prop, PROP_STEREOTYPE_ADD_TABLETYPE, addTableTypeStereotype);
		addSchemaStereotype = Utils.getPropBool(prop, PROP_STEREOTYPE_ADD_SCHEMA, addSchemaStereotype);
		addVertexTypeStereotype = Utils.getPropBool(prop, PROP_STEREOTYPE_ADD_VERTEXTYPE, addVertexTypeStereotype);
		
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
	
	@Override
	public void setPropertiesPrefix(String propertiesPrefix) {
		// TODO: properties-prefix setting
	}
}

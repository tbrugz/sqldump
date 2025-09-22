package tbrugz.sqldump.graph;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.OutputStream;
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

import tbrugz.graphml.model.Edge;
import tbrugz.graphml.model.Root;
import tbrugz.graphml.model.Stereotyped;
import tbrugz.sqldump.dbmodel.Column;
import tbrugz.sqldump.dbmodel.Constraint;
import tbrugz.sqldump.dbmodel.FK;
import tbrugz.sqldump.dbmodel.Index;
import tbrugz.sqldump.dbmodel.Relation;
import tbrugz.sqldump.dbmodel.SchemaModel;
import tbrugz.sqldump.dbmodel.Table;
import tbrugz.sqldump.dbmodel.TableType;
import tbrugz.sqldump.dbmodel.View;
import tbrugz.sqldump.def.AbstractModelDumper;
import tbrugz.sqldump.def.Defs;
import tbrugz.sqldump.def.ProcessingException;
import tbrugz.sqldump.def.SchemaModelDumper;
import tbrugz.sqldump.util.Utils;
import tbrugz.xml.AbstractDump;

enum EdgeLabelType {
	FK, FKANDCOLUMNS, COLUMNS, FKCOLUMNS, PKCOLUMNS, NONE;
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
public class Schema2GraphML extends AbstractModelDumper implements SchemaModelDumper {
	
	static final Log log = LogFactory.getLog(Schema2GraphML.class);
	
	public static final String PREFIX_SCHEMA2GRAPHML = "sqldump.graphmldump";
	public static final String SUFFIX_DUMPFORMATCLASS = ".dumpformatclass";
	
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
	public static final String PROP_NODESTEREOTYPECONTAINS_PREFIX = "sqldump.graphmldump.nodestereotypecontains.";
	
	static final String DEFAULT_SNIPPETS = "/graphml-snippets-sqld.properties";
	static final Class<?> DEFAULT_DUMPFORMAT_CLASS = DumpSchemaGraphMLModel.class;

	Class<?> dumpFormatClass = null;
	String snippetsFile;
	
	File output;
	PrintStream outputStream;
	List<String> schemaNamesList = new ArrayList<String>();
	EdgeLabelType edgeLabel = EdgeLabelType.NONE;
	boolean showSchemaName = true;
	boolean showConstraints = false;
	boolean showIndexes = false;
	
	boolean addViewsAsNodes = true; //XXX prop for showing (or not) views as nodes
	boolean addTableTypeStereotype = true;
	boolean addSchemaStereotype = false;
	boolean addVertexTypeStereotype = false;
	
	Map<String, List<Pattern>> stereotypeRegexes = new HashMap<String, List<Pattern>>();
	Map<String, List<String>> stereotypeNodenames = new HashMap<String, List<String>>();
	
	//String defaultSchemaName;
	
	public Root getGraphMlModel(SchemaModel schemaModel) {
		Root graphModel = new Root();
		
		Set<String> tableIds = new HashSet<String>();
		
		for(Table t: schemaModel.getTables()) {
			TableNode n = processRelation(schemaModel, t);
			tableIds.add(n.getId());
			graphModel.getChildren().add(n);
		}

		for(View v: schemaModel.getViews()) {
			TableNode n = processRelation(schemaModel, v);
			tableIds.add(n.getId());
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
	
	TableNode processRelation(SchemaModel schemaModel, Relation t) {
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
			if(fk.getPkTable().equals(t.getName())) {
				isRoot = false;
			}
			if(fk.getFkTable().equals(t.getName())) {
				isLeaf = false;
			}
		}
		n.setRoot(isRoot);
		n.setLeaf(isLeaf);

		//column number
		n.setColumnNumber(colCount+methodCount);
		
		//node stereotype
		if(addTableTypeStereotype) {
			if(t instanceof Table) {
				Table tt = (Table) t;
				switch (tt.getType()) {
					case SYSTEM_TABLE: 
					case VIEW:
					case MATERIALIZED_VIEW:
					case EXTERNAL_TABLE:
					case SYNONYM:
						addStereotype(n, "type@"+tt.getType()); break;
					case TABLE:
					default:
						break;
				}
			}
			else {
				addStereotype(n, "type@"+TableType.VIEW);
			}
		}
		
		for(String key: stereotypeNodenames.keySet()) {
			//List<Pattern> patterns = stereotypeRegexes.get(key);
			for(String nodename: stereotypeNodenames.get(key)) {
				if(nodename.equals(t.getName())) {
					addStereotype(n, "nodename@"+key);
				}
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
		
		if(t.getSchemaName()!=null && schemaNamesList.contains(t.getSchemaName())) {
			if(addSchemaStereotype) {
				addStereotype(n, "schema@"+t.getSchemaName());
			}
		}
		else {
			if(addSchemaStereotype) {
				addStereotype(n, "otherschema.schema@"+t.getSchemaName());
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
		
		//tableIds.add(id);
		return n;
		//graphModel.getChildren().add(n);
			
	}
	
	int setTableNodeAttributes(TableNode n, SchemaModel schemaModel, Relation t) {
		//columns
		StringBuilder sbCols = new StringBuilder();
		int colCount = 0;
		if(t instanceof Table) {
			Table tt = (Table) t;
			for(Column c: tt.getColumns()) {
				sbCols.append(c.getDefinition()+"\n");
				colCount++;
			}
		}
		else {
			if(t.getColumnNames()!=null) {
			for(String c: t.getColumnNames()) {
				sbCols.append(c+"\n");
				colCount++;
			}
			}
			else {
				log.warn("getColumnNames is null... TableNode: "+n.getId()+" relation: "+t);
			}
		}
		n.setColumnsDesc(sbCols.toString());
		return colCount;
	}

	int setTableNodeMethods(TableNode n, SchemaModel schemaModel, Relation t) {
		//constraints
		n.setConstraintsDesc("");
		int constrCount = 0;
		if(showConstraints) {
			StringBuilder sbConstraints = new StringBuilder();
			for(Constraint cons: t.getConstraints()) {
				switch(cons.getType()) {
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
			StringBuilder sbIndexes = new StringBuilder();
			for(Index idx: schemaModel.getIndexes()) {
				//log.debug("idx: "+idx+" / t: "+t.name);
				if(idx.getTableName().equals(t.getName())) {
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
				l.setName("("+fk.getFkColumns()+" -> "+fk.getPkColumns()+")"); break;
			case FKANDCOLUMNS:
				//fk label: fk name + columns involved
				l.setName(fk.getName()+" ("+fk.getFkColumns()+" -> "+fk.getPkColumns()+")"); break;
			case FKCOLUMNS:
				l.setName(Utils.join(fk.getFkColumns(),", ")); break;
			case PKCOLUMNS:
				l.setName(Utils.join(fk.getPkColumns(),", ")); break;
			case NONE:
			default:
				l.setName(""); break;
		}
		l.referencesPK = fk.getFkReferencesPK();
		l.composite = fk.getFkColumns().size()>1;
		l.setSource(getSourceId(fk));
		l.setTarget(getTargetId(fk));
		
		return l;
	}

	@Override
	public void dumpSchema(SchemaModel schemaModel) {
		log.info("dumping graphML: translating model");
		if(schemaModel==null) {
			log.error("schemaModel is null!");
			if(failonerror) { throw new ProcessingException("schemaModel is null!"); }
			return;
		}
		Root r = getGraphMlModel(schemaModel);
		log.info("dumping model... [size = "+r.getChildren().size()+"]");

		AbstractDump dg = (AbstractDump) Utils.getClassInstance(dumpFormatClass);
		
		try {
			if(outputStream==null) {
				if(output==null) {
					String message = "graphml output file [prop '"+PROP_OUTPUTFILE+"'] not defined. won't dump";
					log.error(message);
					if(failonerror) { throw new ProcessingException(message); }
					return;
				}
				Utils.prepareDir(output);
				outputStream = new PrintStream(output);
			}
				
			dg.loadSnippets(DEFAULT_SNIPPETS);
			if(snippetsFile!=null) { dg.loadSnippets(snippetsFile); }
			
			dg.dumpModel(r, outputStream);
			
			if(output!=null) {
				log.info("graphML dumped to: "+output.getAbsolutePath());
			}
			else {
				log.info("graphML dumped");
			}
		} catch (FileNotFoundException e) {
			log.error("error dumping schema: "+e);
			log.debug("error dumping schema", e);
			if(failonerror) { throw new ProcessingException(e); }
		}
	}

	public void setOutput(File output) {
		this.output = output;
	}
	
	@Override
	public void setProperties(Properties prop) {
		String outFileStr = prop.getProperty(PROP_OUTPUTFILE);
		if(outFileStr==null && output==null) {
			log.debug("graphml output file ["+PROP_OUTPUTFILE+"] not defined");
		}
		else {
			setOutput(new File(outFileStr));
		}
		
		dumpFormatClass = getDumpFormatClass(prop, PREFIX_SCHEMA2GRAPHML+SUFFIX_DUMPFORMATCLASS);
		if(dumpFormatClass!=null) {
			log.info("dump format class: "+dumpFormatClass.getName());
		}
		else {
			dumpFormatClass = DEFAULT_DUMPFORMAT_CLASS;
		}
		
		@SuppressWarnings("deprecation")
		String schemaPattern = Utils.getPropWithDeprecated(prop, Defs.PROP_SCHEMAGRAB_SCHEMANAMES, Defs.PROP_DUMPSCHEMAPATTERN, null);
		
		schemaNamesList = new ArrayList<String>();
		
		if(schemaPattern!=null) { 
			String[] schemasArr = schemaPattern.split(",");
			for(String schemaName: schemasArr) {
				schemaNamesList.add(schemaName.trim());
			}
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

		//node stereotype contains
		for(String key: Utils.getKeysStartingWith(prop, PROP_NODESTEREOTYPECONTAINS_PREFIX)) {
			String stereotype = key.substring(PROP_NODESTEREOTYPECONTAINS_PREFIX.length());
			String[] nodenames = prop.getProperty(key).split(",");
			for(String nodename: nodenames) {
				nodename = nodename.trim();
				log.debug("added stereotype table name: stereotype: '"+stereotype+"', name: '"+nodename+"'");
				List<String> patterns = stereotypeNodenames.get(stereotype);
				if(patterns==null) {
					patterns = new ArrayList<String>();
					stereotypeNodenames.put(stereotype, patterns);
				}
				patterns.add(nodename);
			}
		}
		
		showSchemaName = Utils.getPropBool(prop, PROP_SHOWSCHEMANAME, showSchemaName);
		showConstraints = Utils.getPropBool(prop, PROP_SHOWCONSTRAINTS, showConstraints);
		showIndexes = Utils.getPropBool(prop, PROP_SHOWINDEXES, showIndexes);
		//XXX: remove static reference to DumpSchemaGraphMLModel.nodeHeightByColsNumber
		DumpSchemaGraphMLModel.nodeHeightByColsNumber = Utils.getPropBool(prop, PROP_NODEHEIGHTBYCOLSNUMBER, true);
		
		String snippetsFile = prop.getProperty(PROP_SNIPPETS_FILE);
		if(snippetsFile!=null) {
			this.snippetsFile = snippetsFile;
			log.info("snippets-file is '"+this.snippetsFile+"'");
		}
		
		addTableTypeStereotype = Utils.getPropBool(prop, PROP_STEREOTYPE_ADD_TABLETYPE, addTableTypeStereotype);
		addSchemaStereotype = Utils.getPropBool(prop, PROP_STEREOTYPE_ADD_SCHEMA, addSchemaStereotype);
		addVertexTypeStereotype = Utils.getPropBool(prop, PROP_STEREOTYPE_ADD_VERTEXTYPE, addVertexTypeStereotype);
	}
	
	
	static void addStereotype(Stereotyped stereo, String str){
		if(stereo.getStereotype()!=null) {
			stereo.setStereotype(stereo.getStereotype()+"."+str);
		}
		else {
			stereo.setStereotype(str);
		}
	}
	
	public static String getSourceId(FK fk) {
		return fk.getPkTableSchemaName()+"."+fk.getPkTable();
	}

	public static String getTargetId(FK fk) {
		return fk.getFkTableSchemaName()+"."+fk.getFkTable();
	}
	
	@Override
	public void setPropertiesPrefix(String propertiesPrefix) {
		// TODO: properties-prefix setting
	}
	
	/*static Class<?> getDumpFormatClass(Properties prop, String key, Class<?> defaultFormatClass) {
		String dumpFormatClassStr = prop.getProperty(key);
		if(dumpFormatClassStr!=null) {
			Class<?> c = Utils.getClassWithinPackages(dumpFormatClassStr, "tbrugz.graphml", "tbrugz.sqldump.graph", null);
			if(c!=null) {
				return c;
			}
			else {
				log.warn("dump format class '"+dumpFormatClassStr+"' not found. defaulting to '"+DEFAULT_DUMPFORMAT_CLASS.getName()+"'");
			}
		}
		return defaultFormatClass;
	}*/

	static Class<?> getDumpFormatClass(Properties prop, String key) {
		String dumpFormatClassStr = prop.getProperty(key);
		if(dumpFormatClassStr!=null) {
			Class<?> c = Utils.getClassWithinPackages(dumpFormatClassStr, "tbrugz.graphml", "tbrugz.sqldump.graph", null);
			if(c!=null) {
				return c;
			}
			else {
				log.warn("dump format class '"+dumpFormatClassStr+"' not found. defaulting to '"+DEFAULT_DUMPFORMAT_CLASS.getName()+"'");
			}
		}
		return null;
	}

	@Override
	public String getMimeType() {
		return "application/xml";
	}
	
	@Override
	public boolean acceptsOutputStream() {
		return true;
	}
	
	@Override
	public void setOutputStream(OutputStream out) {
		if(out instanceof PrintStream) {
			this.outputStream = (PrintStream) out;
		}
		else {
			this.outputStream = new PrintStream(out);
		}
	}
	
}

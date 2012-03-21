package tbrugz.sqldump.mondrianschema;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;

import org.apache.log4j.Logger;

import tbrugz.mondrian.xsdmodel.Hierarchy;
import tbrugz.mondrian.xsdmodel.Hierarchy.Join;
import tbrugz.mondrian.xsdmodel.Hierarchy.Level;
import tbrugz.mondrian.xsdmodel.PrivateDimension;
import tbrugz.mondrian.xsdmodel.Schema;
import tbrugz.sqldump.SchemaModel;
import tbrugz.sqldump.SchemaModelDumper;
import tbrugz.sqldump.Utils;
import tbrugz.sqldump.dbmodel.Column;
import tbrugz.sqldump.dbmodel.DBIdentifiable;
import tbrugz.sqldump.dbmodel.DBObjectType;
import tbrugz.sqldump.dbmodel.FK;
import tbrugz.sqldump.dbmodel.Table;
import tbrugz.sqldump.util.StringDecorator;

class HierarchyLevelData {
	String levelName;
	String levelColumn;
	String levelNameColumn;
	String levelType;
	String levelTable;

	//String joinPKTable;
	String joinLeftKey;
	String joinRightKey;
}

/*
 * XXXdone: prop for schema-name
 * XXX: prop for defining cube tables ? add done...
 * XXXdone: prop for measures ?
 * XXX: prop for Level.setNameExpression()?
 * TODOne: props for 'n' levels on one dim table (classic star schema - no snowflake)
 * ~XXX: snowflake: option to dump only one hierarchy per dimension: first? last? longest? preferwithtable[X]?
 * XXXdone: addDimForEachHierarchy
 * XXX: level properties
 * XXXdone: add props: 'sqldump.mondrianschema.cube@<cube>.measurecolsregex=measure_.* | amount_.*'
 * XXX: add props: 'sqldump.mondrianschema.measurecolsregex=measure_.* | amount_.*'
 * 
 * XXX: new dumper: suggestions for aggregate tables. for each fact table with X dimensions:
 * - for Y := X-1 to 0
 * -- one aggregate for each combination of (X minus Y) dimensions
 * ? similar to PAD - Pentaho Aggregate Designer ?
 * TODO: equals/contains: equalsIgnoreCase (as option?)
 * TODOne: warning for unused properties
 * ~XXX: tell mondrian to generate sql without quotes? maybe not... http://docs.huihoo.com/mondrian/3.0.4/faq.html#case_sensitive_table_names
 * TODO: sqldump.mondrianschema.ignorefks=table.fk1, table.fk2
 * TODO: option to lowercase column name on Measure.setName(), ... (also dim, cube)
 * ~TODO: more than 1 aggregator per measure: amount_sum, amount_count, ...
 * XXXxx: Level uniqueMembers="true": when?
 * -- If you know that the values of a given level column in the dimension table are unique across all the other values in that column across the parent levels, then set uniqueMembers="true", otherwise, set to "false". / At the top level, this will always be uniqueMembers="true", as there is no parent level.
 */
public class MondrianSchemaDumper implements SchemaModelDumper {
	
	public static final String PROP_MONDRIAN_SCHEMA = "sqldump.mondrianschema";
	public static final String PROP_MONDRIAN_SCHEMA_OUTFILE = "sqldump.mondrianschema.outfile";
	public static final String PROP_MONDRIAN_SCHEMA_FACTTABLES = "sqldump.mondrianschema.facttables";
	public static final String PROP_MONDRIAN_SCHEMA_XTRAFACTTABLES = "sqldump.mondrianschema.xtrafacttables";
	public static final String PROP_MONDRIAN_SCHEMA_NAME = "sqldump.mondrianschema.schemaname";
	public static final String PROP_MONDRIAN_SCHEMA_ONEHIERPERDIM = "sqldump.mondrianschema.onehierarchyperdim";
	public static final String PROP_MONDRIAN_SCHEMA_ADDDIMFOREACHHIERARCHY = "sqldump.mondrianschema.adddimforeachhierarchy";
	public static final String PROP_MONDRIAN_SCHEMA_IGNOREDIMS = "sqldump.mondrianschema.ignoredims";
	public static final String PROP_MONDRIAN_SCHEMA_IGNORECUBEWITHNOMEASURE = "sqldump.mondrianschema.ignorecubewithnomeasure";
	public static final String PROP_MONDRIAN_SCHEMA_IGNORECUBEWITHNODIMENSION = "sqldump.mondrianschema.ignorecubewithnodimension";
	public static final String PROP_MONDRIAN_SCHEMA_PREFERREDLEVELNAMECOLUMNS = "sqldump.mondrianschema.preferredlevelnamecolumns"; // { "label", "name" };
	public static final String PROP_MONDRIAN_SCHEMA_HIERHASALL = "sqldump.mondrianschema.hierarchyhasall";
	public static final String PROP_MONDRIAN_SCHEMA_ADDALLDEGENERATEDIMCANDIDATES = "sqldump.mondrianschema.addalldegeneratedimcandidates";
	public static final String PROP_MONDRIAN_SCHEMA_DEFAULT_MEASURE_AGGREGATORS = "sqldump.mondrianschema.defaultaggregators";
	//public static final String PROP_MONDRIAN_SCHEMA_ALLPOSSIBLEDEGENERATED = "sqldump.mondrianschema.allpossibledegenerated";
	//public static final String PROP_MONDRIAN_SCHEMA_ALL_POSSIBLE_DEGENERATED = "sqldump.mondrianschema.allnondimormeasureasdegenerated";
	
	public static final String[] DEFAULT_MEASURE_AGGREGATORS = {"sum"};
	
	static Logger log = Logger.getLogger(MondrianSchemaDumper.class);
	
	Properties prop;
	List<String> numericTypes = new ArrayList<String>();
	String fileOutput = "mondrian-schema.xml";
	String mondrianSchemaName;
	List<String> factTables = null;
	List<String> extraFactTables = new ArrayList<String>();
	List<String> ignoreDims = new ArrayList<String>();
	boolean oneHierarchyPerDim = false; //oneHierarchyPerFactTableFK
	boolean addDimForEachHierarchy = false;
	boolean ignoreCubesWithNoMeasure = true;
	boolean ignoreCubesWithNoDimension = true;
	boolean hierarchyHasAll = true;
	boolean addAllDegenerateDimCandidates = false;
	
	boolean equalsShouldIgnoreCase = false;
	StringDecorator stringDecorator = new StringDecorator(); //does nothing (for now?)
	
	//FIXedME: property for it, and list of possible column names
	List<String> preferredLevelNameColumns = new ArrayList<String>();
	List<String> defaultAggregators = new ArrayList<String>();
	
	{
		try {
			Properties mondrianProp = new Properties();
			mondrianProp.load(MondrianSchemaDumper.class.getResourceAsStream("/mondrianxsd.properties"));
			String[] ntypes = mondrianProp.getProperty("type.numeric").split(",");
			for(String s: ntypes) {
				numericTypes.add(s.toUpperCase().trim());
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void procProperties(Properties prop) {
		this.prop = prop;
		fileOutput = prop.getProperty(PROP_MONDRIAN_SCHEMA_OUTFILE);
		mondrianSchemaName = prop.getProperty(PROP_MONDRIAN_SCHEMA_NAME, "sqldumpSchema");
		
		String dimTablesStr = prop.getProperty(PROP_MONDRIAN_SCHEMA_FACTTABLES);
		if(dimTablesStr!=null) {
			factTables = new ArrayList<String>();
			String[] strs = dimTablesStr.split(",");
			for(String s: strs) {
				factTables.add(s.trim());
			}
		}
		
		String extraFactTablesStr = prop.getProperty(PROP_MONDRIAN_SCHEMA_XTRAFACTTABLES);
		if(extraFactTablesStr!=null) {
			String[] strs = extraFactTablesStr.split(",");
			for(String s: strs) {
				extraFactTables.add(s.trim());
			}
		}
		
		String ignoreDimsStr = prop.getProperty(PROP_MONDRIAN_SCHEMA_IGNOREDIMS);
		if(ignoreDimsStr!=null) {
			String[] strs = ignoreDimsStr.split(",");
			for(String s: strs) {
				ignoreDims.add(s.trim());
			}
		}
		
		preferredLevelNameColumns = Utils.getStringListFromProp(prop, PROP_MONDRIAN_SCHEMA_PREFERREDLEVELNAMECOLUMNS, ",");
		
		oneHierarchyPerDim = Utils.getPropBool(prop, PROP_MONDRIAN_SCHEMA_ONEHIERPERDIM, oneHierarchyPerDim);
		addDimForEachHierarchy = Utils.getPropBool(prop, PROP_MONDRIAN_SCHEMA_ADDDIMFOREACHHIERARCHY, addDimForEachHierarchy);
		ignoreCubesWithNoMeasure = Utils.getPropBool(prop, PROP_MONDRIAN_SCHEMA_IGNORECUBEWITHNOMEASURE, ignoreCubesWithNoMeasure);
		ignoreCubesWithNoDimension = Utils.getPropBool(prop, PROP_MONDRIAN_SCHEMA_IGNORECUBEWITHNODIMENSION, ignoreCubesWithNoDimension);
		hierarchyHasAll = Utils.getPropBool(prop, PROP_MONDRIAN_SCHEMA_HIERHASALL, hierarchyHasAll);
		addAllDegenerateDimCandidates = Utils.getPropBool(prop, PROP_MONDRIAN_SCHEMA_ADDALLDEGENERATEDIMCANDIDATES, addAllDegenerateDimCandidates);

		List<String> defaultAggTmp = Utils.getStringListFromProp(prop, PROP_MONDRIAN_SCHEMA_DEFAULT_MEASURE_AGGREGATORS, ",");
		if(defaultAggTmp==null) {
			for(String agg: DEFAULT_MEASURE_AGGREGATORS) {
				defaultAggregators.add(agg);
			}
		}
		else {
			defaultAggregators = defaultAggTmp;
		}
	}

	@Override
	public void dumpSchema(SchemaModel schemaModel) {
		if(fileOutput==null) {
			log.warn("prop '"+PROP_MONDRIAN_SCHEMA_OUTFILE+"' not defined");
			return;
		}
		
		Schema schema = new Schema();
		schema.setName(mondrianSchemaName);
		
		//XXX~: snowflake
		//XXX: virtual cubes / shared dimensions
		//XXXdone: degenerate dimensions
		
		for(Table t: schemaModel.getTables()) {
			List<FK> fks = new ArrayList<FK>();
			boolean isRoot = true;
			boolean isLeaf = true;
			for(FK fk: schemaModel.getForeignKeys()) {
				if(fk.pkTable.equals(t.getName())) {
					isRoot = false;
				}
				if(fk.fkTable.equals(t.getName())) {
					isLeaf = false;
					fks.add(fk);
				}
			}
			
			if(!isRoot && !listContains(extraFactTables, t.name)) { continue; } 
			if(factTables!=null) {
				if(!listContains(factTables,t.name)) { continue; }
				else { factTables.remove(t.name); }
			}

			Schema.Cube cube = new Schema.Cube();
			cube.setName(t.name);
			tbrugz.mondrian.xsdmodel.Table xt = new tbrugz.mondrian.xsdmodel.Table();
			xt.setName(t.name);
			xt.setSchema(t.getSchemaName());
			cube.setTable(xt);

			List<String> measureCols = new ArrayList<String>();
			String measureColsStr = prop.getProperty(PROP_MONDRIAN_SCHEMA+".cube@"+stringDecorator.get(t.name)+".measurecols");
			if(measureColsStr!=null) {
				String[] cols = measureColsStr.split(",");
				for(String c: cols) {
					measureCols.add(c.trim());
				}
			}
			List<String> measureColsRegexes = Utils.getStringListFromProp(prop, PROP_MONDRIAN_SCHEMA+".cube@"+stringDecorator.get(t.name)+".measurecolsregex", "\\|");
			
			List<String> degenerateDimCandidates = new ArrayList<String>();
			//columnloop:
			//measures
			for(Column c: t.getColumns()) {
				procMeasure(cube, c, fks, measureCols, measureColsRegexes, degenerateDimCandidates);
			}
			
			if(cube.getMeasure().size()==0) {
				if(ignoreCubesWithNoMeasure) {
					log.info("cube '"+cube.getName()+"' has no measure: ignoring");
					continue;
				}
				log.warn("cube '"+cube.getName()+"' has no measure...");
			}
			
			//dimensions
			for(FK fk: fks) {
				procDimension(cube, fk, degenerateDimCandidates, schemaModel);
			}
			
			//degenerate dimensions - see: http://mondrian.pentaho.com/documentation/schema.php#Degenerate_dimensions
			String degenerateDimColsStr = prop.getProperty(PROP_MONDRIAN_SCHEMA+".cube@"+stringDecorator.get(t.name)+".degeneratedims");
			if(degenerateDimColsStr!=null) {
				String[] cols = degenerateDimColsStr.split(",");
				for(String c: cols) {
					c = c.trim();
					String colname = c;
					boolean containsCol = false;
					for(Column cc: t.getColumns()) {
						if(stringEquals(cc.name, c)) { containsCol = true; colname = cc.name; break; }
					}
					if(!containsCol) {
						log.warn("column for degenerate dimension '"+c+"' not present in table "+t.name);
					}

					degenerateDimCandidates.remove(c);
					cube.getDimensionUsageOrDimension().add(genDegeneratedDim(colname, c));
				}
			}
			
			if(cube.getDimensionUsageOrDimension().size()==0) {
				if(ignoreCubesWithNoDimension) {
					log.info("cube '"+cube.getName()+"' has no dimension: ignoring");
					continue;
				}
				log.warn("cube '"+cube.getName()+"' has no dimensions");
			}
			
			if(degenerateDimCandidates.size()>0) {
				if(addAllDegenerateDimCandidates) {
					log.info("adding degenerated dim candidates for cube '"+cube.getName()+"': "+degenerateDimCandidates);
					for(String c: degenerateDimCandidates) {
						//log.debug("adding degeneraded dim '"+c+" for cube '"+cube.getName()+"'");
						cube.getDimensionUsageOrDimension().add(genDegeneratedDim(c, "degenerate_dim_"+c));
					}
				}
				else {
					log.info("degenerated dim candidates for cube '"+cube.getName()+"': "+degenerateDimCandidates);
				}
			}
			
			schema.getCube().add(cube);
		}
		
		if(factTables!=null && factTables.size()>0) {
			log.warn("fact tables not found: "+Utils.join(factTables, ", "));
		}
		
		//validade '.level' properties
		validateProperties(schemaModel, ".level@", "level table not found: %s");
		//validade '.cube' properties
		validateProperties(schemaModel, ".cube@", "fact table not found: %s");
		
		try {
			jaxbOutput(schema, new File(fileOutput));
		} catch (JAXBException e) {
			log.warn("error dumping schema: "+e);
			log.debug("error dumping schema", e);
		}
	}
	
	void procMeasure(Schema.Cube cube, Column c, List<FK> fks, List<String> measureCols, List<String> measureColsRegexes, List<String> degenerateDimCandidates) {
		//XXXxx: if column is FK, do not add to measures
		boolean ok = true;
		
		for(FK fk: fks) {
			if(fk.fkColumns.contains(c.name)) {
				log.debug("column '"+c.name+"' belongs to FK. ignoring (as measure)");
				ok = false; break;
				//continue columnloop;
			}
		}

		if(!numericTypes.contains(c.type.toUpperCase())) {
			log.debug("not a measure column type: "+c.type);
			ok = false;
		}

		if(measureCols.size()!=0) {
			if(listContains(measureCols, c.name)) { ok = true; }
			else { ok = false; }
		}

		if(measureColsRegexes!=null) {
			ok = false;
			for(String regex: measureColsRegexes) {
				if(c.name.matches(regex.trim())) {
					ok = true; break;
				}
			}
		}
		
		if(!ok) {
			degenerateDimCandidates.add(c.name);
			return;
		}

		if(defaultAggregators==null || defaultAggregators.size()==0) {
			log.warn("no measure aggregators defined [cube = "+cube+"]");
			return;
		}
		if(defaultAggregators.size()==1) {
			addMeasure(cube, c.name, c.name, defaultAggregators.get(0));
			return;
		}
		for(String agg: defaultAggregators) {
			addMeasure(cube, c.name, c.name+"_"+agg, agg);
		}
	}
	
	void addMeasure(Schema.Cube cube, String column, String name, String aggregator) {
		Schema.Cube.Measure measure = new Schema.Cube.Measure();
		measure.setName(name);
		measure.setColumn(column);
		measure.setAggregator(aggregator);
		measure.setVisible(true);
		cube.getMeasure().add(measure);
	}
	
	void procDimension(Schema.Cube cube, FK fk, List<String> degenerateDimCandidates, SchemaModel schemaModel) {
		if(fk.fkColumns.size()>1) {
			log.debug("fk "+fk+" is composite. ignoring");
			return;
		}
		
		//tbrugz.mondrian.xsdmodel.Table pkTable = new tbrugz.mondrian.xsdmodel.Table();
		//pkTable.setSchema(fk.schemaName);
		//pkTable.setName(fk.pkTable);
		
		String dimName = fk.pkTable;
		if(false) {
			dimName = fk.name;
		}
		
		if(listContains(ignoreDims, dimName)) {
			return;
		}

		degenerateDimCandidates.remove(fk.fkColumns.get(0));
		procHierRecursiveInit(schemaModel, cube, fk, dimName);
		
		/*PrivateDimension dim = new PrivateDimension();
		dim.setName(dimName);
		dim.setForeignKey(fk.fkColumns.iterator().next());
		dim.setType("StandardDimension");
		
		List<HierarchyLevelData> levels = new ArrayList<HierarchyLevelData>();
		procHierRecursive(schemaModel, dim, fk, fk.schemaName, fk.pkTable, levels);
		
		if(oneHierarchyPerDim && dim.getHierarchy().size() > 1) {
			for(int i=0; i < dim.getHierarchy().size() ; i++) {
				if(i!=0) { //first
					log.debug("[one hierarchy per dimension; dim='"+dim.getName()+"'] removing hierarchy: "+dim.getHierarchy().get(i).getName());
					dim.getHierarchy().remove(i);
				}
			}	
		}
		//dim.getHierarchy().add(hier);
		
		cube.getDimensionUsageOrDimension().add(dim);*/
	}
	
	PrivateDimension genDegeneratedDim(String column, String dimName) {
		Level level = new Level();
		level.setName(column);
		level.setColumn(column);
		level.setUniqueMembers(true);

		Hierarchy hier = new Hierarchy();
		hier.setName(column);
		hier.setHasAll(hierarchyHasAll);
		hier.getLevel().add(level);
		
		PrivateDimension dim = new PrivateDimension();
		dim.setName(dimName);
		//dim.setType("StandardDimension");
		dim.getHierarchy().add(hier);
		
		return dim;
	}
	
	/*void procHierararchy(SchemaModel schemaModel, String dimName, FK fk, tbrugz.mondrian.xsdmodel.Table pkTable) {
		List<Hierarchy> hiers = new ArrayList<Hierarchy>();
	}*/

	/*
	 * snowflake: travels the 'tree': when reaches a leaf, adds hierarchy
	 */
	void procHierRecursiveInit(SchemaModel schemaModel, Schema.Cube cube, FK fk, String dimName) {
		PrivateDimension dim = new PrivateDimension();
		dim.setName(dimName);
		dim.setForeignKey(fk.fkColumns.iterator().next());
		dim.setType("StandardDimension");
		
		List<HierarchyLevelData> levels = new ArrayList<HierarchyLevelData>();
		procHierRecursive(schemaModel, cube, dim, fk, fk.getSchemaName(), fk.pkTable, levels);
		
		if(oneHierarchyPerDim && dim.getHierarchy().size() > 1) {
			for(int i=dim.getHierarchy().size()-1; i >= 0; i--) {
				//XXX one hierarchy per dimension (keep first? last? longest? preferwithtable[X]?)
				if(i!=0) { //first
					log.warn("[one hierarchy per dimension; cube='"+cube.getName()+"'; dim='"+dim.getName()+"'] removing hierarchy: "+dim.getHierarchy().get(i).getName());
					dim.getHierarchy().remove(i);
				}
			}	
		}
		//dim.getHierarchy().add(hier);
		
		if(!addDimForEachHierarchy) {
			cube.getDimensionUsageOrDimension().add(dim);
		}
	}
	
	void procHierRecursive(SchemaModel schemaModel, Schema.Cube cube, PrivateDimension dim, FK fk, String schemaName, 
			String pkTableName, List<HierarchyLevelData> levelsData) {
		List<HierarchyLevelData> thisLevels = new ArrayList<HierarchyLevelData>();
		thisLevels.addAll(levelsData);
		boolean isLevelLeaf = true;

		Table pkTable = DBIdentifiable.getDBIdentifiableByTypeSchemaAndName(schemaModel.getTables(), DBObjectType.TABLE, schemaName, fk.pkTable);
		if(pkTable==null) {
			log.warn("table not found: "+schemaName+"."+fk.pkTable);
		}
		
		HierarchyLevelData level = new HierarchyLevelData();
		level.levelName = pkTableName;
		String levelNameColumn = prop.getProperty(PROP_MONDRIAN_SCHEMA+".level@"+stringDecorator.get(level.levelName)+".levelnamecol");
		if(levelNameColumn!=null) {
			if(pkTable.getColumn(levelNameColumn)!=null) {
				level.levelNameColumn = levelNameColumn;
			}
			else {
				log.warn("levelName column not found: "+levelNameColumn+" [table = "+schemaName+"."+fk.pkTable+"]");
			}
		}
		else if(preferredLevelNameColumns!=null) {
			if(pkTable!=null) {
				//checking if column exists
				for(String colName: preferredLevelNameColumns) {
					if(pkTable.getColumn(colName)!=null) {
						log.debug("level column name: "+colName);
						level.levelNameColumn = colName;
						break;
					}
				}
			}
			else {
				log.warn("table not found [2]: "+schemaName+"."+fk.pkTable);
			}
		}
		level.levelColumn = fk.pkColumns.iterator().next();
		level.levelType = "Regular";
		level.joinLeftKey = fk.fkColumns.iterator().next();
		level.joinRightKey = fk.pkColumns.iterator().next();
		//level.joinPKTable = fk.pkTable;
		level.levelTable = fk.pkTable;
		
		log.debug("fkk: "+fk.toStringFull());
		
		thisLevels.add(level);
		//levels.add(level);

		for(FK fkInt: schemaModel.getForeignKeys()) {
			if(fkInt.fkTable.equals(pkTableName) && (fkInt.fkColumns.size()==1)) {
				isLevelLeaf = false;
				procHierRecursive(schemaModel, cube, dim, fkInt, schemaName, fkInt.pkTable, thisLevels);
				//isLeaf = false;
				//fks.add(fkInt);
			}
		}
		
		if(isLevelLeaf) {
			Hierarchy hier = new Hierarchy();
			String hierName = "";
			//hier.setPrimaryKey(fk.pkColumns.iterator().next());
			hier.setPrimaryKey(thisLevels.get(0).levelColumn);
			hier.setHasAll(hierarchyHasAll);
			
			//Table / Join
			if(thisLevels.size()==1) {
				tbrugz.mondrian.xsdmodel.Table table = new tbrugz.mondrian.xsdmodel.Table();
				table.setSchema(schemaName);
				table.setName(pkTableName);
				
				hier.setTable(table);
			}
			else {
				//hier.setPrimaryKeyTable(fk.pkTable); //FIXedME!
				hier.setPrimaryKeyTable(thisLevels.get(0).levelTable);
				
				Join lastJoin = null;
				for(int i=0; i < thisLevels.size(); i++) {
					HierarchyLevelData l = thisLevels.get(i);

					tbrugz.mondrian.xsdmodel.Table table = new tbrugz.mondrian.xsdmodel.Table();
					table.setSchema(schemaName);
					table.setName(l.levelName);

					Join join = new Join();
					join.getRelation().add(table);

					if(i+1 == thisLevels.size()) {
						log.debug("no nextlevel?");
					}
					else {
						HierarchyLevelData nextl = thisLevels.get(i+1);
						join.setLeftKey(nextl.joinLeftKey);
						join.setRightKey(nextl.joinRightKey);
					}

					//if(i!=0) {
					if(lastJoin!=null) {
						//FIXedME must be like fk.fkColumns.iterator().next()
						//lastJoin.setRightKey(l.joinRightKey);
						//thisLevels.get(i-1).getColumn(); 
					}
					
					if(i==0) { hier.setJoin(join); }
					else if(i==thisLevels.size()-1) { lastJoin.getRelation().add(table); }
					else { lastJoin.getRelation().add(join); }
					
					lastJoin = join;
				}
			}
			
			//Levels
			int levelCounter = 0;
			for(int i = thisLevels.size()-1; i>=0; i--) {
				levelCounter++;
				//HierarchyLevelData xlevel = thisLevels.get(i);
				Level l = cloneAsLevel(thisLevels.get(i));
				log.debug("add level: "+l.getName());
				int numOfParentLevels = 0;
				if(levelCounter == 1) {
					numOfParentLevels = createParentLevels(hier, thisLevels.get(i).levelTable);
				}
				if((numOfParentLevels==0) && (levelCounter == 1)) {
					l.setUniqueMembers(true);
				}
				hier.getLevel().add(l);
				if(hierName.length() > 0) {
					hierName += "+";
				}
				hierName += l.getName();
			}
			hier.setName(hierName);
			log.debug("add hier: "+hier.getName());
			
			if(addDimForEachHierarchy) {
				PrivateDimension newDim = new PrivateDimension();
				//dim.setName(dimName);
				newDim.setForeignKey(dim.getForeignKey());
				newDim.setType(dim.getType());
				newDim.getHierarchy().add(hier);
				newDim.setName(hier.getName());
				cube.getDimensionUsageOrDimension().add(newDim);
			}
			else {
				dim.getHierarchy().add(hier);
			}
		}
	}
	
	static Level cloneAsLevel(HierarchyLevelData l) {
		Level lret = new Level();
		lret.setColumn(l.levelColumn);
		lret.setName(l.levelName);
		lret.setNameColumn(l.levelNameColumn);
		lret.setLevelType(l.levelType);
		lret.setTable(l.levelTable);
		//XXX: lret.setNameExpression(value)
		return lret;
	}
	
	//TODO: test column existence
	int createParentLevels(Hierarchy hier, String levelTable) {
		String parentLevels = prop.getProperty(PROP_MONDRIAN_SCHEMA+".level@"+stringDecorator.get(levelTable)+".parentLevels");
		if(parentLevels==null) {
			return 0;
		}
		String[] levelPairs = parentLevels.split(",");
		int count = 0;
		for(String pair: levelPairs) {
			count++;
			String[] tuple = pair.split(":");
			String column = tuple[0].trim();

			Level level = new Level();
			level.setName(column);
			level.setColumn(column);
			level.setTable(levelTable);
			if(tuple[1]!=null) {
				String nameColumn = tuple[1].trim();
				level.setNameColumn(nameColumn);
			}
			if(count==1) {
				level.setUniqueMembers(true);
			}
			//level.setUniqueMembers(true);
			hier.getLevel().add(level);
		}
		return count;
	}
	
	void validateProperties(SchemaModel schemaModel, String appendStr, String messageFormat) {
		List<String> list = Utils.getKeysStartingWith(prop, PROP_MONDRIAN_SCHEMA+appendStr);
		int len = (PROP_MONDRIAN_SCHEMA+appendStr).length();
		Set<String> declaredProps = new HashSet<String>();

		for(String s: list) {
			s = s.substring(len);
			s = s.substring(0, s.indexOf("."));
			log.debug("add prop: "+s);
			declaredProps.add(s);
		}
		for(String s: declaredProps) {
			Table table = DBIdentifiable.getDBIdentifiableByTypeAndName(schemaModel.getTables(), DBObjectType.TABLE, s);
			if(table==null) {
				log.warn(String.format(messageFormat, s));
			}
		}
	}
	
	void jaxbOutput(Object o, File fileOutput) throws JAXBException {
		JAXBContext jc = JAXBContext.newInstance( "tbrugz.mondrian.xsdmodel" );
		Marshaller m = jc.createMarshaller();
		m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);		
		m.marshal(o, fileOutput);
		log.info("mondrian schema model dumped to '"+fileOutput+"'");
	}
	
	boolean stringEquals(String s1, String s2) {
		return equalsShouldIgnoreCase?s1.equalsIgnoreCase(s2):s1.equals(s2);
	}

	boolean listContains(List<String> list, String s) {
		for(String ss: list) {
			if(equalsShouldIgnoreCase?ss.equalsIgnoreCase(s):ss.equals(s)) { return true; }
		}
		return false;
	}
	
	@Override
	public void setPropertiesPrefix(String propertiesPrefix) {
	}
}

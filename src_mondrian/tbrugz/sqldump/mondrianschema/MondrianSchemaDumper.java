package tbrugz.sqldump.mondrianschema;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

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
 * TODO: warning for unused properties
 * XXX: tell mondrian to generate sql without quotes? maybe not... http://docs.huihoo.com/mondrian/3.0.4/faq.html#case_sensitive_table_names
 * TODO: sqldump.mondrianschema.ignorefks=table.fk1, table.fk2
 * TODO: option to lowercase column name on Measure.setName(), ... (also dim, cube)
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
	//public static final String PROP_MONDRIAN_SCHEMA_ALLPOSSIBLEDEGENERATED = "sqldump.mondrianschema.allpossibledegenerated";
	//public static final String PROP_MONDRIAN_SCHEMA_ALL_POSSIBLE_DEGENERATED = "sqldump.mondrianschema.allnondimormeasureasdegenerated";
	
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
	
	//FIXedME: property for it, and list of possible column names
	List<String> preferredLevelNameColumns = new ArrayList<String>();
	
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
		
		String extraDimTablesStr = prop.getProperty(PROP_MONDRIAN_SCHEMA_XTRAFACTTABLES);
		if(extraDimTablesStr!=null) {
			String[] strs = extraDimTablesStr.split(",");
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
			
			if(!isRoot && !extraFactTables.contains(t.name)) { continue; } 
			if(factTables!=null) {
				if(!factTables.contains(t.name)) { continue; }
				else { factTables.remove(t.name); }
			}

			Schema.Cube cube = new Schema.Cube();
			cube.setName(t.name);
			tbrugz.mondrian.xsdmodel.Table xt = new tbrugz.mondrian.xsdmodel.Table();
			xt.setName(t.name);
			xt.setSchema(t.getSchemaName());
			cube.setTable(xt);

			List<String> measureCols = new ArrayList<String>();
			String measureColsStr = prop.getProperty(PROP_MONDRIAN_SCHEMA+".cube@"+t.name+".measurecols");
			if(measureColsStr!=null) {
				String[] cols = measureColsStr.split(",");
				for(String c: cols) {
					measureCols.add(c.trim());
				}
			}
			List<String> measureColsRegexes = Utils.getStringListFromProp(prop, PROP_MONDRIAN_SCHEMA+".cube@"+t.name+".measurecolsregex", "\\|");
			
			List<String> degenerateDimCandidates = new ArrayList<String>();
			//columnloop:
			//measures
			for(Column c: t.getColumns()) {
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
					if(measureCols.contains(c.name)) { ok = true; }
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
					continue;
				}

				Schema.Cube.Measure measure = new Schema.Cube.Measure();
				measure.setName(c.name);
				measure.setColumn(c.name);
				measure.setAggregator("sum");
				measure.setVisible(true);
				cube.getMeasure().add(measure);
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
				if(fk.fkColumns.size()>1) {
					log.debug("fk "+fk+" is composite. ignoring");
					continue;
				}
				
				//tbrugz.mondrian.xsdmodel.Table pkTable = new tbrugz.mondrian.xsdmodel.Table();
				//pkTable.setSchema(fk.schemaName);
				//pkTable.setName(fk.pkTable);
				
				String dimName = fk.pkTable;
				if(false) {
					dimName = fk.name;
				}
				
				if(ignoreDims.contains(dimName)) {
					continue;
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
			//degenerate dimensions - see: http://mondrian.pentaho.com/documentation/schema.php#Degenerate_dimensions
			String degenerateDimColsStr = prop.getProperty(PROP_MONDRIAN_SCHEMA+".cube@"+t.name+".degeneratedims");
			if(degenerateDimColsStr!=null) {
				String[] cols = degenerateDimColsStr.split(",");
				for(String c: cols) {
					c = c.trim();
					boolean containsCol = false;
					for(Column cc: t.getColumns()) {
						if(cc.name.equals(c)) { containsCol = true; }
					}
					if(!containsCol) {
						log.warn("column for degenerate dimension '"+c+"' not present in table "+t.name);
					}

					degenerateDimCandidates.remove(c);
					cube.getDimensionUsageOrDimension().add(genDegeneratedDim(c, c));
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
		
		try {
			jaxbOutput(schema, new File(fileOutput));
		} catch (JAXBException e) {
			log.warn("error dumping schema: "+e);
			log.debug("error dumping schema", e);
		}
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

		HierarchyLevelData level = new HierarchyLevelData();
		level.levelName = pkTableName;
		String levelNameColumn = prop.getProperty(PROP_MONDRIAN_SCHEMA+".level@"+level.levelName+".levelnamecol");
		if(levelNameColumn!=null) {
			level.levelNameColumn = levelNameColumn;
		}
		else if(preferredLevelNameColumns!=null) {
			Table pkTable = DBIdentifiable.getDBIdentifiableByTypeSchemaAndName(schemaModel.getTables(), DBObjectType.TABLE, schemaName, fk.pkTable);
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
				log.warn("table not found: "+schemaName+"."+fk.pkTable);
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
				tbrugz.mondrian.xsdmodel.Table pkTable = new tbrugz.mondrian.xsdmodel.Table();
				pkTable.setSchema(schemaName);
				pkTable.setName(pkTableName);
				
				hier.setTable(pkTable);
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
			for(int i = thisLevels.size()-1; i>=0; i--) {
				//HierarchyLevelData xlevel = thisLevels.get(i);
				Level l = cloneAsLevel(thisLevels.get(i));
				log.debug("add level: "+l.getName());
				createLevels(hier, thisLevels.get(i).levelTable);
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
	
	void createLevels(Hierarchy hier, String levelTable) {
		String parentLevels = prop.getProperty(PROP_MONDRIAN_SCHEMA+".level@"+levelTable+".parentLevels");
		if(parentLevels==null) {
			return;
		}
		String[] levelPairs = parentLevels.split(",");
		for(String pair: levelPairs) {
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
			//level.setUniqueMembers(true);
			hier.getLevel().add(level);
		}
	}
	
	void jaxbOutput(Object o, File fileOutput) throws JAXBException {
		JAXBContext jc = JAXBContext.newInstance( "tbrugz.mondrian.xsdmodel" );
		Marshaller m = jc.createMarshaller();
		m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);		
		m.marshal(o, fileOutput);
		log.info("mondrian schema model dumped to '"+fileOutput+"'");
	}

	@Override
	public void setPropertiesPrefix(String propertiesPrefix) {
	}
}

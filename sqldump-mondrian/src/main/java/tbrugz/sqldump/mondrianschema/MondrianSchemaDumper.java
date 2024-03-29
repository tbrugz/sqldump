package tbrugz.sqldump.mondrianschema;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;

//import mondrian.olap.Mondrian3Def.*;
import mondrian.olap.MondrianDef.Closure;
import mondrian.olap.MondrianDef.Cube;
import mondrian.olap.MondrianDef.CubeDimension;
import mondrian.olap.MondrianDef.Dimension;
import mondrian.olap.MondrianDef.Hierarchy;
import mondrian.olap.MondrianDef.Join;
import mondrian.olap.MondrianDef.Level;
import mondrian.olap.MondrianDef.Measure;
import mondrian.olap.MondrianDef.Schema;
//import mondrian.olap.MondrianDef.Table;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eigenbase.xom.ElementDef;
import org.eigenbase.xom.XMLOutput;
import org.eigenbase.xom.XOMException;
import org.eigenbase.xom.XOMUtil;

import tbrugz.sqldump.dbmodel.Column;
import tbrugz.sqldump.dbmodel.Constraint;
import tbrugz.sqldump.dbmodel.DBIdentifiable;
import tbrugz.sqldump.dbmodel.DBObjectType;
import tbrugz.sqldump.dbmodel.FK;
import tbrugz.sqldump.dbmodel.SchemaModel;
import tbrugz.sqldump.dbmodel.Table;
import tbrugz.sqldump.def.AbstractFailable;
import tbrugz.sqldump.def.ProcessingException;
import tbrugz.sqldump.def.Processor;
import tbrugz.sqldump.def.SchemaModelDumper;
import tbrugz.sqldump.util.ParametrizedProperties;
import tbrugz.sqldump.util.StringDecorator;
import tbrugz.sqldump.util.Utils;

class HierarchyLevelData {
	String levelName;
	String levelColumn;
	String levelNameColumn;
	String levelType;
	String levelTable;
	String levelTableSchema;
	String levelColumnDataType; //Levels contain a type attribute, which can have values "String", "Integer", "Numeric", "Boolean", "Date", "Time", and "Timestamp". The default value is "Numeric" because key columns generally have a numeric type

	//String joinPKTable;
	String joinLeftKey;
	String joinRightKey;
	
	RecursiveHierData recursiveHierarchy;
	
	@Override
	public String toString() {
		return levelName;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((levelColumn == null) ? 0 : levelColumn.hashCode());
		result = prime * result
				+ ((levelName == null) ? 0 : levelName.hashCode());
		result = prime * result
				+ ((levelNameColumn == null) ? 0 : levelNameColumn.hashCode());
		result = prime * result
				+ ((levelTable == null) ? 0 : levelTable.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		HierarchyLevelData other = (HierarchyLevelData) obj;
		if (levelColumn == null) {
			if (other.levelColumn != null)
				return false;
		} else if (!levelColumn.equals(other.levelColumn))
			return false;
		if (levelName == null) {
			if (other.levelName != null)
				return false;
		} else if (!levelName.equals(other.levelName))
			return false;
		if (levelNameColumn == null) {
			if (other.levelNameColumn != null)
				return false;
		} else if (!levelNameColumn.equals(other.levelNameColumn))
			return false;
		if (levelTable == null) {
			if (other.levelTable != null)
				return false;
		} else if (!levelTable.equals(other.levelTable))
			return false;
		return true;
	}
	
}

class RecursiveHierData {
	String levelParentColumn;
	String levelNullParentValue;

	// closure properties
	String closureTable;
	String closureParentColumn;
	String closureChildColumn;
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
 * XXXdone: add props: 'sqldump.mondrianschema.measurecolsregex=measure_.* | amount_.*'
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
 * XXX: uniqueMembers="true": maybe on 1st level of 1st dim table also... 
 * XXX: show parentLevel candidates
 * TODO: option to use/add shared dimensions - to simplify, declare all as shared? or delclare all shared, and if dim is referenced by only one cube, add only to it?
 */
public class MondrianSchemaDumper extends AbstractFailable implements SchemaModelDumper {
	
	public static final String PREFIX_MONDRIANSCHEMA = "sqldump.mondrianschema";
	
	public static final String PROP_MONDRIAN_SCHEMA = "sqldump.mondrianschema";
	public static final String PROP_MONDRIAN_SCHEMA_OUTFILE = "sqldump.mondrianschema.outfile";
	public static final String PROP_MONDRIAN_SCHEMA_VALIDATE = "sqldump.mondrianschema.validateschema";
	
	public static final String PROP_MONDRIAN_SCHEMA_FACTTABLES = "sqldump.mondrianschema.facttables";
	public static final String PROP_MONDRIAN_SCHEMA_XTRAFACTTABLES = "sqldump.mondrianschema.xtrafacttables";
	public static final String PROP_MONDRIAN_SCHEMA_NAME = "sqldump.mondrianschema.schemaname";
	//public static final String PROP_MONDRIAN_SCHEMA_ONEHIERPERDIM = "sqldump.mondrianschema.onehierarchyperdim";
	public static final String PROP_MONDRIAN_SCHEMA_ADDDIMFOREACHHIERARCHY = "sqldump.mondrianschema.adddimforeachhierarchy";
	public static final String PROP_MONDRIAN_SCHEMA_IGNOREDIMS = "sqldump.mondrianschema.ignoredims";
	public static final String PROP_MONDRIAN_SCHEMA_IGNORECUBEWITHNOMEASURE = "sqldump.mondrianschema.ignorecubewithnomeasure";
	public static final String PROP_MONDRIAN_SCHEMA_IGNORECUBEWITHNODIMENSION = "sqldump.mondrianschema.ignorecubewithnodimension";
	public static final String PROP_MONDRIAN_SCHEMA_PREFERREDLEVELNAMECOLUMNS = "sqldump.mondrianschema.preferredlevelnamecolumns"; // { "label", "name" };
	public static final String PROP_MONDRIAN_SCHEMA_HIERHASALL = "sqldump.mondrianschema.hierarchyhasall";
	public static final String PROP_MONDRIAN_SCHEMA_ADDALLDEGENERATEDIMCANDIDATES = "sqldump.mondrianschema.addalldegeneratedimcandidates";
	public static final String PROP_MONDRIAN_SCHEMA_DEFAULT_MEASURE_AGGREGATORS = "sqldump.mondrianschema.defaultaggregators";
	public static final String PROP_MONDRIAN_SCHEMA_SQLID_DECORATOR = "sqldump.mondrianschema.sqliddecorator";
	public static final String PROP_MONDRIAN_SCHEMA_FACTCOUNTMEASURE = "sqldump.mondrianschema.factcountmeasure";
	public static final String PROP_MONDRIAN_SCHEMA_DETECTPARENTCHILDHIER = "sqldump.mondrianschema.detectparentchild";
	public static final String PROP_MONDRIAN_SCHEMA_IGNOREMEASURECOLUMNSFROMPK = "sqldump.mondrianschema.ignoremeasurecolumnsbelongingtopk";
	public static final String PROP_MONDRIAN_SCHEMA_LEVELNAME_PATTERN = "sqldump.mondrianschema.levelname.pattern";
	public static final String PROP_MONDRIAN_SCHEMA_SNOWFLAKE_MAXLEVEL = "sqldump.mondrianschema.snowflake.maxlevel";
	public static final String PROP_MONDRIANSCHEMA_MEASURESCAPTION = PREFIX_MONDRIANSCHEMA+".measuresCaption";
	public static final String PROP_MONDRIANSCHEMA_HIER_ALLMEMBERCAPTIONPATTERN = PREFIX_MONDRIANSCHEMA+".hier.allMemberCaptionPattern";
	
	public static final String SUFFIX_MEASURECOLSREGEX = ".measurecolsregex";

	//public static final String PROP_MONDRIAN_SCHEMA_ALLPOSSIBLEDEGENERATED = "sqldump.mondrianschema.allpossibledegenerated";
	//public static final String PROP_MONDRIAN_SCHEMA_ALL_POSSIBLE_DEGENERATED = "sqldump.mondrianschema.allnondimormeasureasdegenerated";
	
	//possible aggregate functions: "sum", "count", "min", "max", "avg" and "distinct-count" ; mode, median, first, last, concat?
	static final String[] DEFAULT_MEASURE_AGGREGATORS = {"sum"};
	
	static String PATTERN_STR_TABLENAME = "\\[tablename\\]";
	static String PATTERN_STR_PKCOLUMN = "\\[pkcolumn\\]";
	static String PATTERN_STR_FKCOLUMN = "\\[fkcolumn\\]";

	static String PATTERN_STR_DIMNAME = "\\[dimname\\]";
	static String PATTERN_STR_DIMCAPTION = "\\[dimcaption\\]";
	
	static String DEFAULT_LEVELNAME_PATTERN = "[tablename]";
	
	static Log log = LogFactory.getLog(MondrianSchemaDumper.class);
	
	static List<String> numericTypes = new ArrayList<String>();
	static List<String> integerTypes = new ArrayList<String>();

	Properties prop;
	String fileOutput = "mondrian-schema.xml";
	Writer outputWriter;
	boolean validateSchema = false;
	
	String mondrianSchemaName;
	List<String> factTables = null;
	List<String> extraFactTables = new ArrayList<String>();
	List<String> ignoreDims = new ArrayList<String>();
	//boolean oneHierarchyPerDim = false; //oneHierarchyPerFactTableFK
	boolean addDimForEachHierarchy = false;
	boolean ignoreCubesWithNoMeasure = true;
	boolean ignoreCubesWithNoDimension = true;
	boolean hierarchyHasAll = true;
	boolean addAllDegenerateDimCandidates = false;
	boolean detectParentChildHierarchies = true;
	boolean setLevelType = true; //XXX: add prop?
	boolean ignoreMeasureColumnsBelongingToPK = true;
	int maxSnowflakeLevel = -1;
	boolean lowerLevelsAreUnique = true; // if all non-parent (ie: lowest level for each table) levels should have Level.uniqueMembers=true
	
	boolean equalsShouldIgnoreCase = false;
	StringDecorator propIdDecorator = new StringDecorator(); //does nothing (for now?)
	StringDecorator sqlIdDecorator = null;
	String levelNamePattern = DEFAULT_LEVELNAME_PATTERN;
	
	List<String> preferredLevelNameColumns = new ArrayList<String>();
	List<String> defaultAggregators = new ArrayList<String>();
	
	{
		try {
			Properties mondrianProp = new ParametrizedProperties();
			mondrianProp.load(MondrianSchemaDumper.class.getResourceAsStream("mondrianxsd.properties"));
			numericTypes = Utils.getStringListFromProp(mondrianProp, "type.numeric", ",");
			integerTypes = Utils.getStringListFromProp(mondrianProp, "type.integer", ",");
			//log.debug("numeric types: "+numericTypes);
		} catch (IOException e) {
			log.warn("init: IOException: "+e.getMessage(), e);
		}
	}

	@Override
	public void setProperties(Properties prop) {
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
		
		//oneHierarchyPerDim = Utils.getPropBool(prop, PROP_MONDRIAN_SCHEMA_ONEHIERPERDIM, oneHierarchyPerDim);
		//if(prop.getProperty(PROP_MONDRIAN_SCHEMA_ONEHIERPERDIM)!=null) {
		//	log.warn("prop '"+PROP_MONDRIAN_SCHEMA_ONEHIERPERDIM+"' not available");
		//}
		
		addDimForEachHierarchy = Utils.getPropBool(prop, PROP_MONDRIAN_SCHEMA_ADDDIMFOREACHHIERARCHY, addDimForEachHierarchy);
		ignoreCubesWithNoMeasure = Utils.getPropBool(prop, PROP_MONDRIAN_SCHEMA_IGNORECUBEWITHNOMEASURE, ignoreCubesWithNoMeasure);
		ignoreCubesWithNoDimension = Utils.getPropBool(prop, PROP_MONDRIAN_SCHEMA_IGNORECUBEWITHNODIMENSION, ignoreCubesWithNoDimension);
		hierarchyHasAll = Utils.getPropBool(prop, PROP_MONDRIAN_SCHEMA_HIERHASALL, hierarchyHasAll);
		addAllDegenerateDimCandidates = Utils.getPropBool(prop, PROP_MONDRIAN_SCHEMA_ADDALLDEGENERATEDIMCANDIDATES, addAllDegenerateDimCandidates);
		detectParentChildHierarchies = Utils.getPropBool(prop, PROP_MONDRIAN_SCHEMA_DETECTPARENTCHILDHIER, detectParentChildHierarchies);
		ignoreMeasureColumnsBelongingToPK = Utils.getPropBool(prop, PROP_MONDRIAN_SCHEMA_IGNOREMEASURECOLUMNSFROMPK, ignoreMeasureColumnsBelongingToPK);

		List<String> defaultAggTmp = Utils.getStringListFromProp(prop, PROP_MONDRIAN_SCHEMA_DEFAULT_MEASURE_AGGREGATORS, ",");
		if(defaultAggTmp==null) {
			for(String agg: DEFAULT_MEASURE_AGGREGATORS) {
				defaultAggregators.add(agg);
			}
		}
		else {
			defaultAggregators = defaultAggTmp;
		}
		
		{
			String decoratorStr = prop.getProperty(PROP_MONDRIAN_SCHEMA_SQLID_DECORATOR);
			sqlIdDecorator = StringDecorator.getDecorator(decoratorStr);
			log.debug("sql id decorator: "+sqlIdDecorator.getClass().getSimpleName());
		}
		
		validateSchema = Utils.getPropBool(prop, PROP_MONDRIAN_SCHEMA_VALIDATE, validateSchema);
		levelNamePattern = prop.getProperty(PROP_MONDRIAN_SCHEMA_LEVELNAME_PATTERN, levelNamePattern);
		maxSnowflakeLevel = Utils.getPropInt(prop, PROP_MONDRIAN_SCHEMA_SNOWFLAKE_MAXLEVEL, maxSnowflakeLevel);
	}

	@Override
	public void dumpSchema(SchemaModel schemaModel) {
		try {
			dumpSchemaInternal(schemaModel);
		} catch (XOMException e) {
			log.warn("dumpSchema: error: "+e);
			log.debug("dumpSchema: error: "+e.getMessage(), e);
		}
	}
	
	void dumpSchemaInternal(SchemaModel schemaModel) throws XOMException {
		if(fileOutput==null && outputWriter==null) {
			log.error("prop '"+PROP_MONDRIAN_SCHEMA_OUTFILE+"' not defined");
			if(failonerror) { throw new ProcessingException("prop '"+PROP_MONDRIAN_SCHEMA_OUTFILE+"' not defined"); }
			return;
		}
		
		Schema schema = new Schema();
		schema.name = mondrianSchemaName;
		//XXX schema.metamodelVersion ? "3.6"?
		String measuresCaption = prop.getProperty(PROP_MONDRIANSCHEMA_MEASURESCAPTION);
		if(measuresCaption!=null) {
			log.info("schema.measuresCaption: '"+measuresCaption+"'");
			schema.measuresCaption = measuresCaption;
		}
		
		//XXX~: snowflake
		//XXX: virtual cubes / shared dimensions
		
		//add FKs to model based on properties (experimental)
		//TODO: remove & use SchemaModelTransformer
		List<FK> addFKs = new ArrayList<FK>();
		for(Table t: schemaModel.getTables()) {
			List<String> xtraFKs = Utils.getStringListFromProp(prop, PROP_MONDRIAN_SCHEMA+".table@"+propIdDecorator.get(t.getName())+".xtrafk", ",");
			if(xtraFKs==null) { continue; }
			
			for(String newfkstr: xtraFKs) {
				String[] parts = newfkstr.split(":");
				if(parts.length<3) {
					log.warn("wrong number of FK parts: "+parts.length+"; should be 3 or 4 [fkstr='"+newfkstr+"']");
					continue;
				}
				FK fk = new FK();
				log.debug("new FK: "+newfkstr+"; parts.len: "+parts.length);
				fk.setFkTable(t.getName());
				fk.setFkTableSchemaName(t.getSchemaName());
				fk.setFkColumns(Utils.newStringList(parts[0]));
				if(parts[1].contains(".")) {
					String[] pkTableParts = parts[1].split("\\.");
					log.debug("FKschema: "+parts[1]+"; pkTable.len: "+pkTableParts.length);
					fk.setPkTableSchemaName(pkTableParts[0]);
					parts[1] = pkTableParts[1];
				}
				fk.setPkTable(parts[1]);
				fk.setPkColumns(Utils.newStringList(parts[2]));
				if(parts.length>3) {
					fk.setName(parts[3]);
				}
				else {
					fk.setName(fk.getFkTable() + "_" + fk.getPkTable() + "_FK"); //XXX: suggestAcronym?
				}
				
				//TODO: changing model... should clone() first
				addFKs.add(fk);
			}
		}
		schemaModel.getForeignKeys().addAll(addFKs);
		
		for(Table t: schemaModel.getTables()) {
			List<FK> fks = new ArrayList<FK>();
			boolean isRoot = true;
			@SuppressWarnings("unused")
			boolean isLeaf = true;
			
			for(FK fk: schemaModel.getForeignKeys()) {
				if(fk.getPkTable().equals(t.getName())) {
					isRoot = false;
				}
				if(fk.getFkTable().equals(t.getName())) {
					isLeaf = false;
					fks.add(fk);
				}
			}
			
			if(!isRoot && !listContains(extraFactTables, t.getName())) { continue; } 
			if(factTables!=null) {
				if(!listContains(factTables,t.getName())) { continue; }
				else { factTables.remove(t.getName()); }
			}

			Cube cube = new Cube();
			cube.name = t.getName();
			mondrian.olap.MondrianDef.Table xt = new mondrian.olap.MondrianDef.Table();
			xt.name = sqlIdDecorator.get( t.getName() );
			xt.schema = t.getSchemaName();
			cube.fact = xt;

			List<String> measureCols = new ArrayList<String>();
			String measureColsStr = prop.getProperty(PROP_MONDRIAN_SCHEMA+".cube@"+propIdDecorator.get(t.getName())+".measurecols");
			if(measureColsStr!=null) {
				String[] cols = measureColsStr.split(",");
				for(String c: cols) {
					measureCols.add(c.trim());
					log.debug("cube "+cube.name+": add measure: "+c.trim());
				}
			}
			List<String> measureColsRegexes = Utils.getStringListFromProp(prop, PROP_MONDRIAN_SCHEMA+".cube@"+propIdDecorator.get(t.getName())+SUFFIX_MEASURECOLSREGEX, "\\|");
			if(measureColsRegexes==null) {
				measureColsRegexes = Utils.getStringListFromProp(prop, PROP_MONDRIAN_SCHEMA+SUFFIX_MEASURECOLSREGEX, "\\|");
			}
			
			List<String> degenerateDimCandidates = new ArrayList<String>();
			//columnloop:
			//measures
			for(Column c: t.getColumns()) {
				procMeasures(cube, t, c, fks, measureCols, measureColsRegexes, degenerateDimCandidates);
			}
			if(cube.measures==null && measureCols.size()>0) {
				log.warn("no measures from '.measurecols' found: "+measureCols);
			}
			else if(cube.measures!=null && measureCols.size()>cube.measures.length) {
				log.warn("only "+cube.measures.length+" measures from '.measurecols' ("+measureCols+") found");
			}
			
			List<Measure> measures = new ArrayList<Measure>();
			
			List<String> addMeasures = Utils.getStringListFromProp(prop, PROP_MONDRIAN_SCHEMA+".cube@"+propIdDecorator.get(cube.name)+".addmeasures", ";"); //<column>:<aggregator>[:<label>]
			if(addMeasures!=null) {
				for(String addM: addMeasures) {
					String[] parts = addM.split(":");
					if(parts.length<2) {
						log.warn("addmeasures for cube '"+cube.name+"' not well defined: "+addM);
						continue;
					}
					String label = parts.length>=3?parts[2]:parts[0]+"_"+parts[1]; 
					measures.add(newMeasure(parts[0], label, parts[1]));
				}
			}
			
			if(measures.size()>0) {
				cube.measures = concatenate(cube.measures, measures.toArray(new Measure[0]));
			}
			
			int cubeMeasuresCount = cube.measures==null?0:cube.measures.length;
			
			if(cubeMeasuresCount==0) {
				if(ignoreCubesWithNoMeasure) {
					log.info("cube '"+cube.name+"' has no measure: ignoring");
					continue;
				}
				
				log.warn("cube '"+cube.name+"' has no measure...");
			}

			//fact-count measure
			String descFactCountMeasure = prop.getProperty(PROP_MONDRIAN_SCHEMA+".cube@"+propIdDecorator.get(cube.name)+".factcountmeasure",
					prop.getProperty(PROP_MONDRIAN_SCHEMA_FACTCOUNTMEASURE));
			if(descFactCountMeasure!=null) {
				Measure factCountMeasure = getFactCountMeasure(t, descFactCountMeasure);
				if(factCountMeasure!=null) {
					cube.measures = concatenate(cube.measures, new Measure[]{factCountMeasure});
				}
			}
			
			//dimensions
			if(maxSnowflakeLevel!=0) {
			for(FK fk: fks) {
				//XXX: what if multiple FKs point to same table?
				procDimension(cube, fk, degenerateDimCandidates, schemaModel);
			}
			}
			else if(fks.size()>0) {
				log.info("max snowflake level is 0: only degenerated dimensions will be created [cube:"+cube.name+"]");
			}
			
			//degenerate dimensions - see: http://mondrian.pentaho.com/documentation/schema.php#Degenerate_dimensions
			String degenerateDimColsStr = prop.getProperty(PROP_MONDRIAN_SCHEMA+".cube@"+propIdDecorator.get(t.getName())+".degeneratedims");
			if(degenerateDimColsStr!=null) {
				String[] cols = degenerateDimColsStr.split(",");
				for(String col: cols) {
					col = col.trim();
					String nameColumn = null;
					String[] colAndColName = col.split(":");
					if(colAndColName.length>1) {
						col = colAndColName[0];
						nameColumn = colAndColName[1];
					}
					String finalCol = null;
					String finalNameCol = null;
					for(Column cc: t.getColumns()) {
						if(cc.getName().equalsIgnoreCase(col)) { finalCol = cc.getName(); }
						if(nameColumn!=null && cc.getName().equalsIgnoreCase(nameColumn)) { finalNameCol = cc.getName(); }
					}
					if(finalCol==null) {
						log.warn("column for degenerate dimension '"+col+"' not present in table "+t.getName());
						break;
					}
					if(nameColumn!=null && finalNameCol==null) {
						log.warn("name column '"+nameColumn+"' for degenerate dimension '"+col+"' not present in table "+t.getName());
					}

					degenerateDimCandidates.remove(col);
					if(nameColumn!=null) { degenerateDimCandidates.remove(nameColumn); }
					cube.dimensions = concatenate(cube.dimensions, new Dimension[]{genDegeneratedDim(finalCol, finalNameCol, col)});
				}
			}
			
			int cubeDimensionsCount = cube.dimensions==null?0:cube.dimensions.length;
			
			if(cubeDimensionsCount==0) {
				if(ignoreCubesWithNoDimension) {
					log.info("cube '"+cube.name+"' has no dimension: ignoring");
					continue;
				}
				log.warn("cube '"+cube.name+"' has no dimensions");
			}
			
			if(degenerateDimCandidates.size()>0) {
				if(addAllDegenerateDimCandidates) {
					log.info("adding degenerated dim candidates for cube '"+cube.name+"': "+degenerateDimCandidates);
					for(String c: degenerateDimCandidates) {
						//log.debug("adding degeneraded dim '"+c+" for cube '"+cube.getName()+"'");
						cube.dimensions = concatenate(cube.dimensions, new Dimension[]{genDegeneratedDim(c, null, "degenerate_dim_"+c)});
					}
				}
				else {
					log.info("degenerated dim candidates for cube '"+cube.name+"': "+degenerateDimCandidates);
				}
			}
			
			schema.cubes = concatenate(schema.cubes, new Cube[]{cube});
		}
		
		if(factTables!=null && factTables.size()>0) {
			log.warn("fact tables not found: "+Utils.join(factTables, ", "));
		}
		
		//validade '.level' properties
		validateProperties(schemaModel, ".level@", "level table not found: %s");
		//validade '.cube' properties
		validateProperties(schemaModel, ".cube@", "fact table not found: %s");

		//so that schema isn't modified
		schemaModel.getForeignKeys().removeAll(addFKs);
		
		cleanUpBeforeSerialization(schema);
		
		setPropertiesBeforeSerialization(schema);
		
		try {
			log.info("dumping mondrian schema model...");
			if(fileOutput!=null) {
				File fout = new File(fileOutput);
				Utils.prepareDir(fout);
				
				//jaxbOutput(schema, new File(fileOutput));
				xomOutput(schema, fout);
				log.info("mondrian schema model dumped to '"+fout.getAbsolutePath()+"'");
			}
			else {
				xomOutput(schema, outputWriter);
				log.info("mondrian schema model dumped");
			}
			
			if(validateSchema) {
				Processor msv = new MondrianSchemaValidator();
				msv.setProperties(prop);
				msv.process();
			}
		} catch (Exception e) {
			log.warn("error dumping schema: "+e);
			log.debug("error dumping schema", e);
			if(failonerror) { throw new ProcessingException(e); }
		}
	}
	
	Measure getFactCountMeasure(Table t, String descFactCountMeasure) throws XOMException {
		Constraint c = t.getPKConstraint();
		if(c==null) {
			List<Column> cols = t.getColumns();
			Column notNullCol = null;
			for(Column col: cols) {
				if(!col.isNullable()) { notNullCol = col; }
			}
			if(notNullCol!=null) {
				return newMeasure(notNullCol.getName(), descFactCountMeasure, "count");
			}
			else {
				log.warn("table '"+t.getName()+"' has no PK nor not-null-column for fact count measure");
			}
		}
		else {
			String pk1stCol = c.getUniqueColumns().get(0);
			return newMeasure(pk1stCol, descFactCountMeasure, "count");
		}
		return null;
	}
	
	Set<String> noMeasureTypeWarned = new TreeSet<String>();
	
	void procMeasures(Cube cube, Table t, Column c, List<FK> fks, List<String> measureCols, List<String> measureColsRegexes, List<String> degenerateDimCandidates) throws XOMException {
		//XXXxx: if column is FK, do not add to measures
		boolean ok = true;
		
		for(FK fk: fks) {
			if(fk.getFkColumns().contains(c.getName())) {
				log.debug("column '"+c.getName()+"' belongs to FK. ignoring (as measure)");
				ok = false; break;
			}
		}
		
		Constraint pk = t.getPKConstraint();
		if(ignoreMeasureColumnsBelongingToPK && pk!=null && pk.getUniqueColumns().contains(c.getName())) {
			log.info("column '"+c.getName()+"' belongs to PK. ignoring (as measure)");
			ok = false;
		}

		if(c.getType()!=null) {
			String colName = c.getType().toUpperCase();
			if(!numericTypes.contains(colName)) {
				if(noMeasureTypeWarned.contains(colName)) {}
				else {
					log.debug("not a measure column type: "+c.getType());
					noMeasureTypeWarned.add(colName);
				}
				ok = false;
			}
		}
		else {
			log.warn("null column type: "+t.getName()+"."+c.getName());
			ok = false;
		}

		if(measureCols.size()!=0) {
			if(listContains(measureCols, c.getName())) { ok = true; }
			else { ok = false; }
		}
		
		if(measureColsRegexes!=null) {
			ok = false;
			for(String regex: measureColsRegexes) {
				if(Pattern.compile(regex.trim(), Pattern.CASE_INSENSITIVE).matcher(c.getName()).matches()) {
				//if(c.name.matches(regex.trim())) {
					ok = true; break;
				}
			}
		}
		
		if(!ok) {
			degenerateDimCandidates.add(c.getName());
			return;
		}
		
		List<String> aggs = Utils.getStringListFromProp(prop, PROP_MONDRIAN_SCHEMA+".cube@"+propIdDecorator.get(cube.name)+".aggregators", ",");
		if(aggs==null || aggs.size()==0) { aggs = defaultAggregators; }

		if(aggs==null || aggs.size()==0) {
			log.warn("no measure aggregators defined [cube = "+cube+"]");
			return;
		}

		List<Measure> newMeasures = new ArrayList<Measure>();
		if(aggs.size()==1) {
			newMeasures.add(newMeasure(c.getName(), c.getName(), defaultAggregators.get(0)));
		}
		else {
			for(String agg: aggs) {
				newMeasures.add(newMeasure(c.getName(), c.getName()+"_"+agg, agg));
			}
		}
		cube.measures = concatenate(cube.measures, newMeasures.toArray(new Measure[0]));
	}
	
	Measure newMeasure(String column, String name, String aggregator) throws XOMException {
		Measure measure = new Measure();
		measure.name = name;
		measure.column = sqlIdDecorator.get(column);
		measure.aggregator = aggregator;
		measure.visible = true;
		return measure;
	}
	
	void procDimension(Cube cube, FK fk, List<String> degenerateDimCandidates, SchemaModel schemaModel) throws XOMException {
		if(fk.getFkColumns().size()>1) {
			log.debug("fk "+fk+" is composite. ignoring");
			return;
		}
		
		//tbrugz.mondrian.xsdmodel.Table pkTable = new tbrugz.mondrian.xsdmodel.Table();
		//pkTable.setSchema(fk.schemaName);
		//pkTable.setName(fk.pkTable);
		
		String dimName = fk.getPkTable();
		/*if(false) {
			dimName = fk.getName();
		}*/
		
		if(listContains(ignoreDims, dimName)) {
			return;
		}

		degenerateDimCandidates.remove(fk.getFkColumns().get(0));
		procHierRecursiveInit(schemaModel, cube, fk, dimName);
	}
	
	Dimension genDegeneratedDim(String column, String nameColumn, String dimName) throws XOMException {
		Level level = new Level();
		level.name = column;
		level.column = sqlIdDecorator.get(column);
		if(nameColumn!=null) {
			level.nameColumn = sqlIdDecorator.get(nameColumn);
		}
		level.uniqueMembers = true;

		Hierarchy hier = new Hierarchy();
		hier.name = column;
		hier.hasAll = hierarchyHasAll;
		hier.levels = concatenate(hier.levels, new Level[]{level});
		
		Dimension dim = new Dimension();
		dim.name = dimName;
		//dim.setType("StandardDimension");
		dim.hierarchies = concatenate(dim.hierarchies, new Hierarchy[]{hier});
		
		return dim;
	}
	
	/*
	 * snowflake: travels the 'tree': when reaches a leaf, adds hierarchy
	 */
	void procHierRecursiveInit(SchemaModel schemaModel, Cube cube, FK fk, String dimName) throws XOMException {
		Dimension dim = new Dimension();
		dim.name = dimName;
		dim.foreignKey = sqlIdDecorator.get( fk.getFkColumns().iterator().next() );
		dim.type = "StandardDimension";
		
		List<HierarchyLevelData> levels = new ArrayList<HierarchyLevelData>();
		procHierRecursive(schemaModel, cube, dim, fk, levels);
		
		//TODO: re-enable oneHierarchyPerDim
		/*if(oneHierarchyPerDim && dim.getHierarchy().size() > 1) {
			for(int i=dim.getHierarchy().size()-1; i >= 0; i--) {
				//XXX one hierarchy per dimension (keep first? last? longest? preferwithtable[X]?)
				if(i!=0) { //first
					log.warn("[one hierarchy per dimension; cube='"+cube.getName()+"'; dim='"+dim.getName()+"'] removing hierarchy: "+dim.getHierarchy().get(i).getName());
					dim.getHierarchy().remove(i);
				}
			}	
		}*/
		//dim.getHierarchy().add(hier);
		
		if(!addDimForEachHierarchy) {
			cube.dimensions = concatenate(cube.dimensions, new Dimension[]{dim});
		}
	}
	
	HierarchyLevelData getHierLevelData(Table pkTable, FK fk) {
		HierarchyLevelData level = new HierarchyLevelData();
		String tableName = pkTable.getName();
		String schemaName = pkTable.getSchemaName();
		
		String levelNameColumn = prop.getProperty(PROP_MONDRIAN_SCHEMA+".level@"+propIdDecorator.get(tableName)+".levelnamecol");
		if(levelNameColumn!=null) {
			Column c = pkTable.getColumnIgnoreCase(levelNameColumn);
			if(c!=null) {
				level.levelNameColumn = c.getName();
			}
			else {
				log.warn("levelName column not found: "+levelNameColumn+" [table = "+schemaName+"."+fk.getPkTable()+"]");
			}
		}
		else if(preferredLevelNameColumns!=null) {
			//checking if column exists
			for(String colName: preferredLevelNameColumns) {
				Column c = pkTable.getColumnIgnoreCase(colName);
				if(c!=null) {
					log.debug("level column name: "+c.getName());
					level.levelNameColumn = c.getName();
					break;
				}
			}
		}
		String pkColStr = fk.getPkColumns().iterator().next();
		String fkColStr = fk.getFkColumns().iterator().next();
		level.levelColumn = pkColStr;
		level.levelType = "Regular";
		level.joinLeftKey = fkColStr;
		level.joinRightKey = pkColStr;
		//level.joinPKTable = fk.pkTable;
		level.levelTable = fk.getPkTable();
		level.levelTableSchema = fk.getPkTableSchemaName();
		Column pkCol = pkTable.getColumn(pkColStr);
		if(setLevelType) { setLevelType(level, pkCol); }

		// parent-child hierarchies properties
		String levelParentColumn = prop.getProperty(PROP_MONDRIAN_SCHEMA+".level@"+propIdDecorator.get(tableName)+".levelparentcol");
		if(levelParentColumn!=null) {
			level.recursiveHierarchy = new RecursiveHierData();
			level.recursiveHierarchy.levelParentColumn = levelParentColumn;
			
			getParentChildHierInfo(level);
		}
		
		//levelnamepattern may use [tablename];[pkcolumn];[fkcolumn]
		level.levelName = getLevelName(tableName, pkColStr, fkColStr);
		
		log.debug("fk: "+fk.toStringFull());
		
		return level;
	}
	
	void getParentChildHierInfo(HierarchyLevelData level) {
		String levelNullParentVal = prop.getProperty(PROP_MONDRIAN_SCHEMA+".level@"+propIdDecorator.get(level.levelTable)+".levelnullparentvalue");
		if(levelNullParentVal!=null) {
			level.recursiveHierarchy.levelNullParentValue = levelNullParentVal;
		}
		String levelClosure = prop.getProperty(PROP_MONDRIAN_SCHEMA+".level@"+propIdDecorator.get(level.levelTable)+".closure");
		if(levelClosure!=null) {
			String[] parts = levelClosure.split(":"); //table:parent-column:child-column
			level.recursiveHierarchy.closureTable = parts[0];
			level.recursiveHierarchy.closureParentColumn = parts[1];
			level.recursiveHierarchy.closureChildColumn = parts[2];
		}
	}
	
	void procHierRecursive(SchemaModel schemaModel, Cube cube, Dimension dim, FK fk, 
			List<HierarchyLevelData> levelsData) throws XOMException {
		List<HierarchyLevelData> thisLevels = new ArrayList<HierarchyLevelData>();
		thisLevels.addAll(levelsData);
		boolean isLevelLeaf = true;
		String pkTableName = fk.getPkTable();
		String schemaName = fk.getPkTableSchemaName();

		Table pkTable = DBIdentifiable.getDBIdentifiableByTypeSchemaAndName(schemaModel.getTables(), DBObjectType.TABLE, schemaName, pkTableName);
		if(pkTable==null) {
			log.warn("table not found: "+schemaName+"."+pkTableName);
			return;
		}
		
		HierarchyLevelData getHierLevelData = getHierLevelData(pkTable, fk);
		//XXX if hierLevel with same name already exists, rename?
		thisLevels.add(getHierLevelData);

		for(FK fkInt: schemaModel.getForeignKeys()) {
			if(!fkInt.getFkTable().equals(pkTableName)) { continue; }
			if(fkInt.getFkColumns().size()!=1) { continue; }
			if(fkInt.getPkTable().equals(pkTableName)) {
				//XXX auto (parent-child?) relationship
				if(detectParentChildHierarchies) {
					if(getHierLevelData.recursiveHierarchy==null) {
						getHierLevelData.recursiveHierarchy = new RecursiveHierData();
					}
					if(getHierLevelData.recursiveHierarchy.levelParentColumn==null) {
						getHierLevelData.recursiveHierarchy.levelParentColumn = fkInt.getFkColumns().get(0);
					}
					getParentChildHierInfo(getHierLevelData);
				}
				else {
					//isLevelLeaf = false;
					log.debug("leaf-level auto-relation ignored [cube:"+cube.name+"]: levels="+thisLevels+" ; fkcol="+fkInt.getFkColumns().get(0));
				}
			}
			else {
				if(maxSnowflakeLevel>=0 && thisLevels.size()>=maxSnowflakeLevel) {
					log.debug("max snowflake level reached ["+maxSnowflakeLevel+"], levels: "+thisLevels); //info or debug?
				}
				else if(isCycle(thisLevels)) {
					log.debug("cycle detected: "+thisLevels);
				}
				else {
					isLevelLeaf = false;
					procHierRecursive(schemaModel, cube, dim, fkInt, thisLevels);
				}
				//isLeaf = false;
				//fks.add(fkInt);
			}
		}
		
		//boolean lowerLevelsAsDistinctHiers = Utils.getPropBool(prop, PROP_MONDRIAN_SCHEMA+".level@"+propIdDecorator.get(pkTableName)+".addLowerLevelsAsDistinctHiers");
		if(isLevelLeaf) {// || lowerLevelsAsDistinctDim) {
			List<Hierarchy> hiers = makeHierarchies(pkTable, thisLevels);
			addHiersToDim(cube, dim, hiers.toArray(new Hierarchy[0]));			
		}
	}
	
	List<Hierarchy> makeHierarchies(Table pkTable, List<HierarchyLevelData> thisLevels) throws XOMException {
			Hierarchy hier = new Hierarchy();
			//hier.setPrimaryKey(fk.pkColumns.iterator().next());
			hier.primaryKey = sqlIdDecorator.get( thisLevels.get(0).levelColumn );
			hier.hasAll = hierarchyHasAll;
			
			//Table / Join
			if(thisLevels.size()==1) {
				mondrian.olap.MondrianDef.Table table = new mondrian.olap.MondrianDef.Table();
				table.schema = pkTable.getSchemaName();
				table.name = sqlIdDecorator.get( pkTable.getName() );
				
				hier.relation = table;
			}
			else {
				hier.primaryKeyTable = sqlIdDecorator.get( thisLevels.get(0).levelTable );
				
				Join lastJoin = null;
				for(int i=0; i < thisLevels.size(); i++) {
					HierarchyLevelData l = thisLevels.get(i);

					mondrian.olap.MondrianDef.Table table = new mondrian.olap.MondrianDef.Table();
					table.schema = sqlIdDecorator.get( l.levelTableSchema );
					table.name = sqlIdDecorator.get( l.levelTable );

					Join join = new Join();
					join.left = table; //XXX: left or right?

					if(i+1 == thisLevels.size()) {
						log.debug("no nextlevel?");
					}
					else {
						HierarchyLevelData nextl = thisLevels.get(i+1);
						join.leftKey = sqlIdDecorator.get( nextl.joinLeftKey );
						join.rightKey = sqlIdDecorator.get( nextl.joinRightKey );
					}

					//if(i!=0) {
					if(lastJoin!=null) {
						//FIXedME must be like fk.fkColumns.iterator().next()
						//lastJoin.setRightKey(l.joinRightKey);
						//thisLevels.get(i-1).getColumn(); 
					}
					
					if(i==0) {
						hier.relation = join;
					}
					else if(i==thisLevels.size()-1) {
						lastJoin.right = table;
					}
					else {
						lastJoin.right = join;
					}
					
					lastJoin = join;
				}
			}
			
			//Levels
			List<Hierarchy> hiers = new ArrayList<Hierarchy>();
			int levelCounter = 0;
			for(int i = thisLevels.size()-1; i>=0; i--) {
				levelCounter++;
				//HierarchyLevelData xlevel = thisLevels.get(i);
				Level l = cloneAsLevel(thisLevels.get(i));
				log.debug("add level: "+l.name);
				int numOfParentLevels = 0;
				boolean addLowestLevel = true;
				if(levelCounter == 1) {
					String levelTable = thisLevels.get(i).levelTable;
					List<String> parentLevelPairs = Utils.getStringListFromProp(prop, PROP_MONDRIAN_SCHEMA+".level@"+propIdDecorator.get(levelTable)+".levels", ",");
					if(parentLevelPairs!=null) {
						addLowestLevel = false;
					}
					else {
						parentLevelPairs = Utils.getStringListFromProp(prop, PROP_MONDRIAN_SCHEMA+".level@"+propIdDecorator.get(levelTable)+".parentLevels", ",");
					}
					
					Hierarchy hierLower = cloneHierarchy(hier);
					hiers.add(hierLower);
					
					if(parentLevelPairs!=null) {
						//log.info("addParentLevels: pairs="+parentLevelPairs);
						numOfParentLevels = createParentLevels(hierLower, pkTable, parentLevelPairs);
						boolean addLowerLevelsAsDistinctHiers = Utils.getPropBool(prop, PROP_MONDRIAN_SCHEMA+".level@"+propIdDecorator.get(levelTable)+".addLowerLevelsAsDistinctHiers");
						if(addLowerLevelsAsDistinctHiers && parentLevelPairs.size()>0) { //makeLowerLevelsAsDistinctHiers
							//log.info("addLowerLevelsAsDistinctDim: pairs="+parentLevelPairs);
							parentLevelPairs.remove(0); //removing top level
							while(parentLevelPairs.size()>0) {
								Hierarchy hxtra = cloneHierarchy(hier);
								createParentLevels(hxtra, pkTable, parentLevelPairs);
								hiers.add(hxtra);
								parentLevelPairs.remove(0); //removing top level
							}
						}
					}
				}
				
				//XXX: add parentLevels to other levels? "showflake" it?
				//Table parentTable = DBIdentifiable.getDBIdentifiableByTypeSchemaAndName(schemaModel.getTables(), DBObjectType.TABLE, schemaName, thisLevels.get(i).levelTable);
				//numOfParentLevels = createParentLevels(hier, parentTable, thisLevels.get(i).levelTable);
				
				if(addLowestLevel) {
					if(lowerLevelsAreUnique || ((numOfParentLevels==0) && (levelCounter == 1))) {
						l.uniqueMembers = true;
					}
					for(Hierarchy h: hiers) {
						h.levels = concatenate(h.levels, new Level[]{l});
					}
				}
			}
			
			for(Hierarchy h: hiers) {
				setupHierarchyName(h);
				log.debug("add hier: "+h.name);
			}
			
			return hiers;
	}
	
	void addHiersToDim(Cube cube, Dimension dim, Hierarchy[] hiers) {
			if(addDimForEachHierarchy) {
				for(int i=0;i<hiers.length;i++) {
				Hierarchy hier = hiers[i];
				Dimension newDim = new Dimension();
				//dim.setName(dimName);
				newDim.foreignKey = sqlIdDecorator.get( dim.foreignKey );
				newDim.type = dim.type;
				//newDim.hierarchies = concatenate(newDim.hierarchies, new Hierarchy[]{hier});
				newDim.hierarchies = new Hierarchy[]{hier};
				newDim.name = hier.name;
				cube.dimensions = concatenate(cube.dimensions, new Dimension[]{newDim});
				}
			}
			else {
				dim.hierarchies = concatenate(dim.hierarchies, hiers);
			}
	}
	
	Hierarchy cloneHierarchy(Hierarchy hier) {
		Hierarchy newhier = new Hierarchy();
		newhier.primaryKey = hier.primaryKey;
		newhier.hasAll = hier.hasAll;
		newhier.relation = hier.relation;
		newhier.primaryKeyTable = hier.primaryKeyTable;
		newhier.name = hier.name;
		//newhier.levels = hier.levels; //cloned it
		if(hier.levels!=null) {
			newhier.levels = Arrays.copyOf(hier.levels, hier.levels.length);
		}
		return newhier;
	}
	
	boolean isCycle(List<HierarchyLevelData> hierdata) {
		HierarchyLevelData top = hierdata.get(hierdata.size()-1);
		for(int i=hierdata.size()-2;i>=0;i--) {
			if(top.equals(hierdata.get(i))) {
				return true;
			}
		}
		return false;
	}
	
	Level cloneAsLevel(HierarchyLevelData l) throws XOMException {
		Level lret = new Level();
		lret.column = sqlIdDecorator.get( l.levelColumn );
		lret.name = l.levelName;
		lret.nameColumn = sqlIdDecorator.get( l.levelNameColumn );
		lret.levelType = l.levelType;
		lret.table = sqlIdDecorator.get( l.levelTable );
		lret.type = l.levelColumnDataType;
		//XXX: lret.setNameExpression(value)
		if(l.recursiveHierarchy!=null) {
			lret.parentColumn = l.recursiveHierarchy.levelParentColumn;
			lret.uniqueMembers = true; //XXX: always true for parent-child hierarchy?
			if(l.recursiveHierarchy.levelNullParentValue!=null) {
				lret.nullParentValue = l.recursiveHierarchy.levelNullParentValue;
			}
			if(l.recursiveHierarchy.closureTable!=null) {
				Closure closure = new Closure();
				mondrian.olap.MondrianDef.Table t = new mondrian.olap.MondrianDef.Table();
				t.name = l.recursiveHierarchy.closureTable;
				closure.table = t;
				closure.parentColumn = l.recursiveHierarchy.closureParentColumn;
				closure.childColumn = l.recursiveHierarchy.closureChildColumn;
				lret.closure = closure;
			}
			
		}
		return lret;
	}
	
	int createParentLevels(Hierarchy hier, Table table, List<String> levelPairs) throws XOMException {
		/*if(parentLevels==null) {
			return 0;
		}*/
		//String[] levelPairs = parentLevels.split(",");
		//log.info("createParentLevels ["+table+"]: "+levelPairs);
		int count = 0;
		for(String pair: levelPairs) {
			count++;
			String[] tuple = pair.split(":");
			String column = tuple[0].trim();
			Column col = table.getColumnIgnoreCase(column);
			//log.info("level:: '"+column+"' / '"+col+"'");
			if(col==null) {
				log.warn("level column '"+column+"' for table '"+table.getName()+"' does not exist");
				continue;
			}

			Level level = new Level();
			level.name = col.getName();
			level.column = sqlIdDecorator.get( col.getName() );
			level.table = sqlIdDecorator.get( table.getName() );
			if(tuple.length>1) {
				String nameColumn = tuple[1].trim();
				Column levelNameCol = table.getColumnIgnoreCase(nameColumn);
				if(levelNameCol==null) {
					log.warn("level nameColumn '"+nameColumn+"' for table '"+table.getName()+"' does not exist");
				}
				else {
					level.nameColumn = levelNameCol.getName();
				}
			}
			if(count==1) {
				level.uniqueMembers = true;
			}
			if(setLevelType) { setLevelType(level, col); }
			//level.setUniqueMembers(true);
			hier.levels = concatenate(hier.levels, new Level[]{level});
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
			Table table = DBIdentifiable.getDBIdentifiableByTypeAndNameIgnoreCase(schemaModel.getTables(), DBObjectType.TABLE, s);
			if(table==null) {
				log.warn(String.format(messageFormat, s));
			}
		}
	}
	
	@Deprecated
	void jaxbOutput(Object o, File fileOutput) throws JAXBException {
		//JAXBContext jc = JAXBContext.newInstance( "tbrugz.mondrian.xsdmodel" );
		JAXBContext jc = JAXBContext.newInstance( "mondrian.olap" );
		
		Marshaller m = jc.createMarshaller();
		m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);		
		log.info("dumping mondrian schema model...");
		m.marshal(o, fileOutput);
		log.info("mondrian schema model dumped to '"+fileOutput.getAbsolutePath()+"'");
	}

	void xomOutput(ElementDef e, File fileOutput) throws IOException {
		PrintWriter pw = new PrintWriter(fileOutput);
		xomOutput(e, pw);
	}
	
	void xomOutput(ElementDef e, Writer writer) throws IOException {
		XMLOutput out = new XMLOutput(writer);
		e.displayXML(out, 4);
		writer.close();
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
	
	void setupHierarchyName(Hierarchy hier) {
		List<String> levels = new ArrayList<String>();
		for(Level l: hier.levels) {
			levels.add(l.name);
		}
		hier.name = Utils.join(levels, "+");
	}
	
	String getLevelName(String tableName, String pkColumnName, String fkColumnName) {
		return levelNamePattern.replaceAll(PATTERN_STR_TABLENAME, tableName)
			.replaceAll(PATTERN_STR_PKCOLUMN, pkColumnName)
			.replaceAll(PATTERN_STR_FKCOLUMN, fkColumnName);
	}
	
	void setHierarchyAllMemberCaption(Hierarchy hier, String dimName, String dimCaption) {
		String hierAllMemberCaption = prop.getProperty(PROP_MONDRIAN_SCHEMA+".hier@"+hier.name+".allMemberCaption");
		if(hierAllMemberCaption!=null) {
			hier.allMemberCaption = hierAllMemberCaption;
			return;
		}
		String hierAllMemberCaptionPattern = prop.getProperty(PROP_MONDRIANSCHEMA_HIER_ALLMEMBERCAPTIONPATTERN);
		if(hierAllMemberCaptionPattern!=null) {
			if(dimName==null) { dimName = ""; }
			if(dimCaption==null) { dimCaption = ""; }
			hier.allMemberCaption = hierAllMemberCaptionPattern.replaceAll(PATTERN_STR_DIMNAME, dimName)
					.replaceAll(PATTERN_STR_DIMCAPTION, dimCaption);
		}
	}

	void setPropertiesBeforeSerialization(Schema schema) {
		if(schema.cubes==null) { return; }
		for(Cube cube: schema.cubes) {

			// cube caption
			String cubeCaption = prop.getProperty(PROP_MONDRIAN_SCHEMA+".cube@"+cube.name+".caption");
			if(cubeCaption!=null) {
				log.debug("cubeCaption ["+cube.name+"]: "+cubeCaption);
				cube.caption = cubeCaption;
			}
			
			if(cube.dimensions==null) { continue; }
			for(CubeDimension cdim: cube.dimensions) {
				
				// dim caption
				String dimCaption = prop.getProperty(PROP_MONDRIAN_SCHEMA+".dim@"+cdim.name+".caption");
				if(dimCaption!=null) {
					log.debug("dimCaption ["+cdim.name+"]: "+dimCaption);
					cdim.caption = dimCaption;
				}
			
				if(!(cdim instanceof Dimension)) { continue; }
				Dimension dim = (Dimension) cdim;
				if(dim.hierarchies==null) { continue; }
				for(Hierarchy hier: dim.hierarchies) {
					
					// hierarchy all-member caption
					setHierarchyAllMemberCaption(hier, dim.name, dim.caption);
					
					//XXX: warn for tables/columns not found...
					
					if(hier.levels==null) { continue; }
					for(Level l: hier.levels) {
						// level internalType
						String internalType = prop.getProperty(PROP_MONDRIAN_SCHEMA+".table@"+l.table+".column@"+l.column+".internalType");
						if(internalType!=null) {
							log.debug("level.internalType ["+l.table+":"+l.column+"]: "+internalType);
							l.internalType = internalType;
						}
						
						// level caption
						String levelCaption = prop.getProperty(PROP_MONDRIAN_SCHEMA+".level@"+l.name+".caption");
						if(levelCaption!=null) {
							log.debug("levelCaption ["+l.name+"]: "+levelCaption);
							l.caption = levelCaption;
						}
					}
				}
			}
		}
	}
	
	void cleanUpBeforeSerialization(Schema schema) {
		if(schema.cubes==null) { return; }
		for(Cube cube: schema.cubes) {
			
			Map<String, Dimension> cubeDims = new HashMap<String, Dimension>();
		
			if(cube.dimensions==null) { continue; }
			for(CubeDimension cdim: cube.dimensions) {
				
				if(!(cdim instanceof Dimension)) { continue; }
				Dimension dim = (Dimension) cdim;
				
				//test for duplicated Dims
				Dimension prevDim = cubeDims.put(dim.name, dim);
				if(prevDim!=null) {
					log.info("duplicated dim-name found: renaming dim '"+dim.name+"' to FK column '"+dim.foreignKey+"' (other FK column is '"+prevDim.foreignKey+"')");
					dim.name = dim.foreignKey;
					prevDim.name = prevDim.foreignKey;
					// renaming bottom level names & hierarchy names...
					if(dim.hierarchies!=null) {
					for(Hierarchy h: dim.hierarchies) {
						int idx = h.levels.length-1;
						log.info("duplicated dim-name found: renaming dim level '"+h.levels[idx].name+"' to '"+dim.foreignKey+"'");
						h.levels[idx].name = dim.foreignKey; 
						setupHierarchyName(h);
					}
					}
					if(prevDim.hierarchies!=null) {
					for(Hierarchy h: prevDim.hierarchies) {
						int idx = h.levels.length-1;
						log.info("duplicated dim-name found: renaming prev dim level '"+h.levels[idx].name+"' to '"+prevDim.foreignKey+"'");
						h.levels[idx].name = prevDim.foreignKey; 
						setupHierarchyName(h);
					}
					}
				}
				
				Map<String, Hierarchy> dimHiers = new HashMap<String, Hierarchy>();
				
				if(dim.hierarchies==null) { continue; }
				for(Hierarchy hier: dim.hierarchies) {
					
					//test for duplicated Hierarchies
					Hierarchy prevHier = dimHiers.put(hier.name, hier);
					if(prevHier!=null) {
						log.info("duplicated hier-name found: '"+hier.name+"' [cube: '"+cube.name+"']");
					}
					
					/*if(hier.levels==null) { continue; }
					for(Level l: hier.levels) {
					}*/
				}
			}
		}
	}
	
	static void setLevelType(HierarchyLevelData level, Column col) {
		if(isInteger(col)) {
			level.levelColumnDataType = "Integer";
		}
		else if(isNumeric(col)) {
			level.levelColumnDataType = "Numeric";
		}
	}

	void setLevelType(Level level, Column col) {
		if(isInteger(col)) {
			level.type = "Integer";
		}
		else if(isNumeric(col)) {
			level.type = "Numeric";
		}
	}
	
	static boolean isNumeric(Column c) {
		if(c==null || c.getType()==null) { return false; }
		return Utils.getEqualIgnoreCaseFromList(numericTypes, c.getType())!=null;
	}
	
	static boolean isInteger(Column c) {
		if(c==null || c.getType()==null) { return false; }
		return Utils.getEqualIgnoreCaseFromList(integerTypes, c.getType())!=null
				|| (isNumeric(c) && (c.getDecimalDigits()==null || c.getDecimalDigits()<=0));
	}
	
	@SuppressWarnings("unchecked")
	static <T extends Object> T[] concatenate(T[] a, T[] b) {
		if(a==null) return b;
		return (T[]) XOMUtil.concatenate(a, b);
	}
	
	@Override
	public String getMimeType() {
		return "application/xml";
	}
	
	@Override
	public boolean acceptsOutputWriter() {
		return true;
	}
	
	@Override
	public void setOutputWriter(Writer writer) {
		outputWriter = writer;
	}

}

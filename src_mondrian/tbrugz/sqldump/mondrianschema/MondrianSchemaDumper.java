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
import tbrugz.mondrian.xsdmodel.Hierarchy.Level;
import tbrugz.mondrian.xsdmodel.PrivateDimension;
import tbrugz.mondrian.xsdmodel.Schema;
import tbrugz.sqldump.SchemaModel;
import tbrugz.sqldump.SchemaModelDumper;
import tbrugz.sqldump.dbmodel.Column;
import tbrugz.sqldump.dbmodel.FK;
import tbrugz.sqldump.dbmodel.Table;

/*
 * XXXdone: prop for schema-name
 * XXX: prop for defining cube tables ? add done...
 * XXXdone: prop for measures ?
 */
public class MondrianSchemaDumper implements SchemaModelDumper {
	
	public static final String PROP_MONDRIAN_SCHEMA = "sqldump.mondrianschema";
	public static final String PROP_MONDRIAN_SCHEMA_OUTFILE = "sqldump.mondrianschema.outfile";
	public static final String PROP_MONDRIAN_SCHEMA_XTRADIMTABLES = "sqldump.mondrianschema.xtradimtables";
	public static final String PROP_MONDRIAN_SCHEMA_NAME = "sqldump.mondrianschema.schemaname";
	
	static Logger log = Logger.getLogger(MondrianSchemaDumper.class);
	
	Properties prop;
	List<String> numericTypes = new ArrayList<String>();
	String fileOutput = "mondrian-schema.xml";
	String schemaName;
	List<String> extraDimTables = new ArrayList<String>();
	
	{
		try {
			Properties mondrianProp = new Properties();
				mondrianProp.load(MondrianSchemaDumper.class.getResourceAsStream("/mondrianxsd.properties"));
			String[] ntypes = mondrianProp.getProperty("type.numeric").split(",");
			for(String s: ntypes) {
				numericTypes.add(s.trim());
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void procProperties(Properties prop) {
		this.prop = prop;
		fileOutput = prop.getProperty(PROP_MONDRIAN_SCHEMA_OUTFILE);
		schemaName = prop.getProperty(PROP_MONDRIAN_SCHEMA_NAME, "sqldumpSchema");
		String extraDimTablesStr = prop.getProperty(PROP_MONDRIAN_SCHEMA_XTRADIMTABLES);
		if(extraDimTablesStr!=null) {
			String[] strs = extraDimTablesStr.split(",");
			for(String s: strs) {
				extraDimTables.add(s);
			}
		}
	}

	@Override
	public void dumpSchema(SchemaModel schemaModel) throws Exception {
		if(fileOutput==null) {
			log.warn("prop '"+PROP_MONDRIAN_SCHEMA_OUTFILE+"' not defined");
			return;
		}
		
		Schema schema = new Schema();
		schema.setName(schemaName);
		
		//XXX: snowflake
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
			
			if(!isRoot && !extraDimTables.contains(t.name)) continue;

			Schema.Cube cube = new Schema.Cube();
			cube.setName(t.name);
			tbrugz.mondrian.xsdmodel.Table xt = new tbrugz.mondrian.xsdmodel.Table();
			xt.setName(t.name);
			xt.setSchema(t.schemaName);
			cube.setTable(xt);

			List<String> measureCols = new ArrayList<String>();
			String measureColsStr = prop.getProperty(PROP_MONDRIAN_SCHEMA+".cube@"+t.name+".measurecols");
			if(measureColsStr!=null) {
				String[] cols = measureColsStr.split(",");
				for(String c: cols) {
					measureCols.add(c.trim());
				}
			}
			
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

				if(!numericTypes.contains(c.type)) {
					log.debug("not a measure column type: "+c.type);
					ok = false;
				}

				if(measureCols.size()!=0) {
					if(measureCols.contains(c.name)) { ok = true; }
					else { ok = false; }
				}
				if(!ok) continue;

				Schema.Cube.Measure measure = new Schema.Cube.Measure();
				measure.setName(c.name);
				measure.setColumn(c.name);
				measure.setAggregator("sum");
				measure.setVisible(true);
				cube.getMeasure().add(measure);
			}
			
			//dimensions
			for(FK fk: fks) {
				if(fk.fkColumns.size()>1) {
					log.debug("fk "+fk+" is composite. ignoring");
					continue;
				}
				
				tbrugz.mondrian.xsdmodel.Table pkTable = new tbrugz.mondrian.xsdmodel.Table();
				pkTable.setSchema(fk.schemaName);
				pkTable.setName(fk.pkTable);
				
				String dimName = fk.pkTable;
				if(false) {
					dimName = fk.name;
				}
				
				Level level = new Level();
				level.setName(dimName);
				String levelName = prop.getProperty(PROP_MONDRIAN_SCHEMA+".level@"+level.getName()+".levelnamecol");
				if(levelName!=null) {
					level.setNameColumn(levelName);
				}
				level.setColumn(fk.pkColumns.iterator().next());
				level.setLevelType("Regular");
				
				//TODO: snowflake: percorre 'arvore': cada vez q chega numa folha, adiciona hierarquia...
				
				Hierarchy hier = new Hierarchy();
				hier.setName(dimName);
				hier.setPrimaryKey(fk.pkColumns.iterator().next());
				hier.setTable(pkTable);
				hier.getLevel().add(level);

				PrivateDimension dim = new PrivateDimension();
				dim.setName(dimName);
				dim.setForeignKey(fk.fkColumns.iterator().next());
				dim.setType("StandardDimension");
				dim.getHierarchy().add(hier);
				
				cube.getDimensionUsageOrDimension().add(dim);
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
					Level level = new Level();
					level.setName(c);
					level.setColumn(c);
					level.setUniqueMembers(true);

					Hierarchy hier = new Hierarchy();
					hier.setName(c);
					hier.setHasAll(true);
					hier.getLevel().add(level);
					
					PrivateDimension dim = new PrivateDimension();
					dim.setName(c);
					//dim.setType("StandardDimension");
					dim.getHierarchy().add(hier);
					
					cube.getDimensionUsageOrDimension().add(dim);
					//degenerateDimCols.add(c.trim());
				}
			}
			
			schema.getCube().add(cube);
		}
		
		jaxbOutput(schema, new File(fileOutput));
	}
	
	void jaxbOutput(Object o, File fileOutput) throws JAXBException {
		JAXBContext jc = JAXBContext.newInstance( "tbrugz.mondrian.xsdmodel" );
		Marshaller m = jc.createMarshaller();
		m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);		
		m.marshal(o, fileOutput);
		log.info("mondrian schema model dumped to '"+fileOutput+"'");
	}

}

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
 * XXX: prop for schema-name
 * XXX: prop for defining cube tables ?
 * XXX: prop for measures ?
 */
public class MondrianSchemaDumper implements SchemaModelDumper {
	
	public static final String PROP_MONDRIAN_SCHEMA_OUTFILE = "sqldump.mondrianschema.outfile";
	
	static Logger log = Logger.getLogger(MondrianSchemaDumper.class);
	
	List<String> numericTypes = new ArrayList<String>();
	String fileOutput = "mondrian-schema.xml";
	
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
		fileOutput = prop.getProperty(PROP_MONDRIAN_SCHEMA_OUTFILE);
	}

	@Override
	public void dumpSchema(SchemaModel schemaModel) throws Exception {
		
		Schema schema = new Schema();
		schema.setName("schemaName-later");
		
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
			
			if(!isRoot) continue;

			Schema.Cube cube = new Schema.Cube();
			cube.setName(t.name);
			tbrugz.mondrian.xsdmodel.Table xt = new tbrugz.mondrian.xsdmodel.Table();
			xt.setName(t.name);
			xt.setSchema(t.schemaName);
			cube.setTable(xt);
			
			for(Column c: t.getColumns()) {
				//XXX: if column is FK, do not add to measures
				if(numericTypes.contains(c.type)) {
					Schema.Cube.Measure measure = new Schema.Cube.Measure();
					measure.setName(c.name);
					measure.setColumn(c.name);
					measure.setAggregator("sum");
					measure.setVisible(true);
					cube.getMeasure().add(measure);
				}
				else {
					log.debug("not a measure column type: "+c.type);
				}
			}
			
			for(FK fk: fks) {
				if(fk.fkColumns.size()>1) {
					log.debug("fk "+fk+" is composite. ignoring");
					continue;
				}
				
				tbrugz.mondrian.xsdmodel.Table pkTable = new tbrugz.mondrian.xsdmodel.Table();
				pkTable.setSchema(fk.schemaName);
				pkTable.setName(fk.pkTable);
				
				Level level = new Level();
				level.setName(fk.name);
				level.setColumn(fk.pkColumns.iterator().next());
				level.setLevelType("Regular");
				
				Hierarchy hier = new Hierarchy();
				hier.setName(fk.name);
				hier.setPrimaryKey(fk.pkColumns.iterator().next());
				hier.setTable(pkTable);
				hier.getLevel().add(level);

				PrivateDimension dim = new PrivateDimension();
				dim.setName(fk.name);
				dim.setForeignKey(fk.fkColumns.iterator().next());
				dim.setType("StandardDimension");
				dim.getHierarchy().add(hier);
				
				cube.getDimensionUsageOrDimension().add(dim);
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

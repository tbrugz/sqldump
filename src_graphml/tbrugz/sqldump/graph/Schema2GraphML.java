package tbrugz.sqldump.graph;

import java.io.File;
import java.io.PrintStream;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.graphml.DumpGraphMLModel;
import tbrugz.graphml.model.Root;
import tbrugz.graphml.model.Link;
import tbrugz.sqldump.SQLDump;
import tbrugz.sqldump.SchemaModel;
import tbrugz.sqldump.SchemaModelDumper;
import tbrugz.sqldump.dbmodel.Column;
import tbrugz.sqldump.dbmodel.FK;
import tbrugz.sqldump.dbmodel.Table;
import tbrugz.xml.AbstractDump;

/*
 * TODO: show PK columns, FK columns, constraints?
 */
public class Schema2GraphML extends SchemaModelDumper {
	
	static Log log = LogFactory.getLog(AbstractDump.class);
	
	public static final String PROP_OUTPUTFILE = "sqldump.graphmldump.outputfile";

	File output;
	
	public static Root getGraphMlModel(SchemaModel schemaModel) {
		Root graphModel = new Root();
		
		/*Set<FK> fks = schemaModel.getForeignKeys();
		Map<String, Set<FK>> fkMap = new HashMap<String, Set<FK>>(); 
		for(FK fk: fks) {
			String fkId = fk.getSourceId();
			Set<FK> fkSet = fkMap.get(fkId);
			if(fkSet==null) {
				fkSet = new HashSet<FK>();
				fkMap.put(fkId, fkSet);
			}
			fkSet.add(fk);
		}*/
		Set<String> tableIds = new HashSet<String>();
		
		for(Table t: schemaModel.getTables()) {
			TableNode n = new TableNode();
			String id = t.getSchemaName()+"."+t.getName();
			n.setId(id);
			n.setLabel(id);
			StringBuffer sb = new StringBuffer();
			for(Column c: t.getColumns()) {
				sb.append(Column.getColumnDesc(c, null, null, null)+"\n");
			}
			n.setColumnsDesc(sb.toString());
			
			tableIds.add(id);
			graphModel.getChildren().add(n);
			/*Set<FK> fkSet = fkMap.get(id);
			if(fkSet!=null) {
    			for(FK fk: fkSet) {
    				Link l = fkToLink(fk);
    				n.getProx().add(l);
    			}
			}*/
		}
		
		for(FK fk: schemaModel.getForeignKeys()) {
			Link l = fkToLink(fk);
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
	
	static Link fkToLink(FK fk) {
		Link l = new Link();
		l.setName(fk.getName());
		l.setSource(fk.getSourceId());
		l.setTarget(fk.getTargetId());
		return l;
	}

	@Override
	public void dumpSchema(SchemaModel schemaModel) throws Exception {
		log.info("dumping graphML: translating model");
		Root r = Schema2GraphML.getGraphMlModel(schemaModel);
		log.info("dumping model...");
		DumpGraphMLModel dg = new DumpSchemaGraphMLModel();
		dg.dumpModel(r, new PrintStream(output));
		log.info("... graphML dumped");
	}
	
	@Override
	public void setOutput(File output) {
		this.output = output;
	}
	
	@Override
	public void procProperties(Properties prop) {
		String s = prop.getProperty(PROP_OUTPUTFILE);
		setOutput(new File(s));
	}
}

package tbrugz.sqldump.mondrianschema;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.def.ProcessingException;
import tbrugz.sqldump.processors.XSLTProcessor;
import tbrugz.sqldump.util.Utils;

public class MondrianSchema2GraphProcessor extends XSLTProcessor {
	
	static Log log = LogFactory.getLog(MondrianSchema2GraphProcessor.class);

	public static final String PREFIX_MONDRIAN_2GRAPH = "sqldump.mondrianschema2graph";
	public static final String SUFFIX_INFILE = ".infile";
	public static final String SUFFIX_OUTFILE = ".outfile";
	
	String schemaFile = null;
	String graphOutFile = null;
	
	@Override
	public void setProperties(Properties prop) {
		super.setProperties(prop);
		
		schemaFile = prop.getProperty(PREFIX_MONDRIAN_2GRAPH+SUFFIX_INFILE);
		if(schemaFile==null) {
			schemaFile = prop.getProperty(MondrianSchemaDumper.PROP_MONDRIAN_SCHEMA_OUTFILE);
		}
		graphOutFile = prop.getProperty(PREFIX_MONDRIAN_2GRAPH+SUFFIX_OUTFILE);
		
		xsl = MondrianSchema2GraphProcessor.class.getResourceAsStream("mondrianschema2graphml.xsl");
	}
	
	@Override
	public void process() {
		if(schemaFile==null) {
			log.warn("suffix '"+SUFFIX_INFILE+"' nor prop '"+MondrianSchemaDumper.PROP_MONDRIAN_SCHEMA_OUTFILE+"' defined");
			return;
		}
		if(graphOutFile==null) {
			log.warn("suffix '"+SUFFIX_OUTFILE+"' not defined");
			return;
		}
		
		File fout = new File(graphOutFile);
		
		try {
			in = new FileInputStream(schemaFile);
			Utils.prepareDir(fout);
			out = new FileWriter(fout);
		} catch (IOException e) {
			log.warn("error: "+e);
			if(failonerror) {
				throw new ProcessingException(e);
			}
		}
		
		super.process();
		
		log.info("graph written to: "+fout.getAbsolutePath());
	}
	
}

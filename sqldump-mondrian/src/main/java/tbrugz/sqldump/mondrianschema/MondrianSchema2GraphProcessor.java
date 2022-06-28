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
	
	static final Log log = LogFactory.getLog(MondrianSchema2GraphProcessor.class);

	public static final String PREFIX_MONDRIAN_2GRAPH = "sqldump.mondrianschema2graph";
	
	public static final String SUFFIX_INFILE = ".infile";
	public static final String SUFFIX_OUTFILE = ".outfile";
	public static final String SUFFIX_XSL_SIMPLE = ".xsl.simple";
	
	public static final String XSL_DEFAULT = "mondrianschema2graphml.xsl";
	public static final String XSL_SIMPLE = "mondrianschema2graphml-simple.xsl";
	
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
		
		if(Utils.getPropBool(prop, PREFIX_MONDRIAN_2GRAPH+SUFFIX_XSL_SIMPLE)) {
			xsl = MondrianSchema2GraphProcessor.class.getResourceAsStream(XSL_SIMPLE);
			log.info("simple XSL selected (no nodes for hierarchies)");
		}
		else {
			xsl = MondrianSchema2GraphProcessor.class.getResourceAsStream(XSL_DEFAULT);
		}
	}
	
	@Override
	public void process() {
		if(schemaFile==null) {
			log.warn("suffix '"+SUFFIX_INFILE+"' nor prop '"+MondrianSchemaDumper.PROP_MONDRIAN_SCHEMA_OUTFILE+"' defined [prefix is '"+PREFIX_MONDRIAN_2GRAPH+"']");
			return;
		}
		if(graphOutFile==null) {
			log.warn("suffix '"+SUFFIX_OUTFILE+"' not defined [prefix is '"+PREFIX_MONDRIAN_2GRAPH+"']");
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

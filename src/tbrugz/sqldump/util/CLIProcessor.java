package tbrugz.sqldump.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class CLIProcessor {

	//static/constant properties
	public static final String PROP_PROPFILEBASEDIR = "propfilebasedir"; //"propfiledir" / "propfilebasedir" / "propertiesbasedir" / "basepropdir"
	
	//cli parameters
	public static final String PARAM_PROPERTIES_FILENAME = "-propfile=";
	public static final String PARAM_PROPERTIES_RESOURCE = "-propresource=";
	public static final String PARAM_USE_SYSPROPERTIES = "-usesysprop=";
	
	static final Log log = LogFactory.getLog(CLIProcessor.class);
	
	//XXX: move to utils(?)... (used by sqldump & sqlrun -- why not sqldiff?) 
	public static void init(final String productName, final String[] args,
			final String defaultPropFile, final Properties papp) throws IOException {
		log.info((productName!=null?productName+" ":"")+
				"init... [version "+Version.getVersion()+"]");
		boolean useSysPropSetted = false;
		int loadedCount = 0;
		if(args!=null) {
		for(String arg: args) {
			if(arg.indexOf(PARAM_PROPERTIES_FILENAME)==0) {
				String propFilename = arg.substring(PARAM_PROPERTIES_FILENAME.length());
				loadFile(papp, propFilename);
				loadedCount++;
			}
			else if(arg.indexOf(PARAM_PROPERTIES_RESOURCE)==0) {
				String propResource = arg.substring(PARAM_PROPERTIES_RESOURCE.length());
				loadResource(papp, propResource);
				loadedCount++;
			}
			else if(arg.indexOf(PARAM_USE_SYSPROPERTIES)==0) {
				String useSysProp = arg.substring(PARAM_USE_SYSPROPERTIES.length());
				ParametrizedProperties.setUseSystemProperties(useSysProp.equalsIgnoreCase("true"));
				useSysPropSetted = true;
			}
			else {
				log.warn("unrecognized param '"+arg+"'. ignoring...");
			}
		}
		}
		if(loadedCount==0) {
			loadFile(papp, defaultPropFile);
		}
		if(!useSysPropSetted) {
			ParametrizedProperties.setUseSystemProperties(true); //set to true by default
			useSysPropSetted = true;
		}
		log.debug("using sys properties: "+ParametrizedProperties.isUseSystemProperties());
	}
	
	static void loadResource(Properties p, String propResource) {
		log.info("loading properties resource: "+propResource);
		InputStream propIS = CLIProcessor.class.getResourceAsStream(propResource);
		if(propIS==null) {
			log.warn("properties resource '"+propResource+"' does not exist");
		}
		else {
			try {
				p.load(propIS);
				propIS.close();
			}
			catch(IOException e) {
				log.warn("error loading resource '"+propResource+"': "+e);
			}
		}
	}
	
	static void loadFile(Properties p, String propFilename) {
		loadFile(p, propFilename, true);
	}
	
	static void loadFile(Properties p, String propFilename, boolean logExceptionAsWarn) {
		File propFile = new File(propFilename);
		File propFileDir = propFile.getAbsoluteFile().getParentFile();
		log.debug("propfile base dir: "+propFileDir);
		p.setProperty(PROP_PROPFILEBASEDIR, propFileDir.toString());
		
		try {
			log.info("loading properties file: "+propFile);
			InputStream propIS = new FileInputStream(propFile);
			p.load(propIS);
			propIS.close();
		}
		catch(IOException e) {
			if(logExceptionAsWarn) {
				log.warn("error loading file '"+propFile+"': "+e);
			}
			else {
				log.debug("error loading file '"+propFile+"': "+e);
			}
		}
	}

}

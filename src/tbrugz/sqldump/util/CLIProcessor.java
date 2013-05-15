package tbrugz.sqldump.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
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
		boolean propFilenameSetted = false;
		boolean propResourceSetted = false;
		//parse args
		String propFilename = defaultPropFile;
		String propResource = null;
		for(String arg: args) {
			if(arg.indexOf(PARAM_PROPERTIES_FILENAME)==0) {
				propFilename = arg.substring(PARAM_PROPERTIES_FILENAME.length());
				propFilenameSetted = true;
			}
			else if(arg.indexOf(PARAM_PROPERTIES_RESOURCE)==0) {
				propResource = arg.substring(PARAM_PROPERTIES_RESOURCE.length());
				propResourceSetted = true;
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
		if(!useSysPropSetted) {
			ParametrizedProperties.setUseSystemProperties(true); //set to true by default
			useSysPropSetted = true;
		}
		log.debug("using sys properties: "+ParametrizedProperties.isUseSystemProperties());
		
		InputStream propIS = null;
		if(propResourceSetted) {
			//XXX: set PROP_PROPFILEBASEDIR for resources?
			
			log.info("loading properties resource: "+propResource);
			propIS = CLIProcessor.class.getResourceAsStream(propResource);
			if(propIS==null) {
				log.warn("properties resource '"+propResource+"' does not exist");
			}
		}
		else {
			File propFile = new File(propFilename);
			
			//init properties
			File propFileDir = propFile.getAbsoluteFile().getParentFile();
			log.debug("propfile base dir: "+propFileDir);
			papp.setProperty(PROP_PROPFILEBASEDIR, propFileDir.toString());
			
			if(propFile.exists()) {
				log.info("loading properties: "+propFile);
				propIS = new FileInputStream(propFile);
			}
			else {
				if(propFilenameSetted) {
					log.warn("properties file '"+propFile+"' does not exist");
				}
				else {
					log.info("properties file '"+propFile+"' does not exist"); //XXX: change to debug() ?
				}
			}
		}
		try {
			if(propIS!=null) {
				papp.load(propIS);
			}
		}
		catch(FileNotFoundException e) {
			if(propResourceSetted) {
				log.warn("prop resource not found: "+propResource);
			}
			else if(propFilenameSetted) {
				log.warn("prop file not found: "+propFilename);
			}
		}
		/*catch(IOException e) {
			if(propResourceSetted) {
				log.warn("error loading prop resource: "+propResource);
			}
			else if(propFilenameSetted) {
				log.warn("error loading prop file: "+propFilename);
			}
		}*/
	}

}

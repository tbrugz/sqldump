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
	public static final String PARAM_VERSION = "--version";
	public static final String PARAM_HELP = "--help";
	
	static final Log log = LogFactory.getLog(CLIProcessor.class);
	
	//XXX: move to utils(?)... (used by sqldump & sqlrun -- why not sqldiff?) 
	public static void init(final String productName, final String[] args,
			final String defaultPropFile, final Properties papp) throws IOException {
		log.info((productName!=null?productName+" ":"")+
				"init... [version "+Version.getVersion()+"]");
		log.debug("buildNumber: "+Version.getBuildNumber()+" ; buildTimestamp: "+Version.getBuildTimestamp());
		boolean useSysPropSetted = false;
		int loadedCount = 0;
		if(args!=null) {
			for(String arg: args) {
				if(arg.indexOf(PARAM_PROPERTIES_FILENAME)==0) {
					String propFilename = arg.substring(PARAM_PROPERTIES_FILENAME.length());
					boolean loaded = loadFile(papp, propFilename);
					if(loaded) { loadedCount++; }
				}
				else if(arg.indexOf(PARAM_PROPERTIES_RESOURCE)==0) {
					String propResource = arg.substring(PARAM_PROPERTIES_RESOURCE.length());
					boolean loaded = loadResource(papp, propResource);
					if(loaded) { loadedCount++; }
				}
				else if(arg.indexOf(PARAM_USE_SYSPROPERTIES)==0) {
					String useSysProp = arg.substring(PARAM_USE_SYSPROPERTIES.length());
					ParametrizedProperties.setUseSystemProperties(useSysProp.equalsIgnoreCase("true"));
					useSysPropSetted = true;
				}
				else {
					log.warn("unrecognized param '"+arg+"' (ignored). for more information use '"+PARAM_HELP+"' argument");
					//XXX: show help and exit?
				}
			}
		}
		if(loadedCount==0) {
			boolean loaded = loadFile(papp, defaultPropFile, true);
			if(loaded) { loadedCount++; }
		}

		//log.debug(papp.size()+" properties defined ; "+loadedCount+" files or resources read");
		if(papp.size()==0 || (papp.containsKey(PROP_PROPFILEBASEDIR) && papp.size()==1)) {
			log.warn("no properties loaded"+(loadedCount==0?" (and no properties file or resource read)":""));
			//XXX throw exception?
		}
		if(!useSysPropSetted) {
			ParametrizedProperties.setUseSystemProperties(true); //set to true by default
			useSysPropSetted = true;
		}
		log.debug("using sys properties: "+ParametrizedProperties.isUseSystemProperties());
	}
	
	public static boolean shouldStopExec(final String productName, final String[] args) {
		if(args!=null) {
			for(String arg: args) {
				if(arg.equals(PARAM_VERSION)) {
					System.out.println((productName!=null?productName+" ":"")+"version: "+Version.getVersion());
					return true;
				}
				else if(arg.equals(PARAM_HELP)) {
					String out = (productName!=null?productName+" ":"")+"version: "+Version.getVersion()+"\n\n"
						+ "parameters:\n\n"
						+ "\t"+PARAM_PROPERTIES_FILENAME+"<file>: use <file> properties\n"
						+ "\t"+PARAM_PROPERTIES_RESOURCE+"<resource>: use <resource> properties\n"
						+ "\t"+PARAM_USE_SYSPROPERTIES+"[true|false]: use system properties (default is true)\n"
						+ "\t"+PARAM_HELP+": show this help and exit\n"
						+ "\t"+PARAM_VERSION+": show version and exit\n"
						+ "\nmore info at <https://github.com/tbrugz/sqldump>\n";
					System.out.println(out);
					return true;
				}
			}
		}
		return false;
	}
	
	static boolean loadResource(Properties p, String propResource) {
		log.info("loading properties resource: "+propResource);
		InputStream propIS = CLIProcessor.class.getResourceAsStream(propResource);
		if(propIS==null) {
			log.warn("properties resource '"+propResource+"' does not exist");
			return false;
		}
		try {
			p.load(propIS);
			propIS.close();
			return true;
		}
		catch(IOException e) {
			log.warn("error loading resource '"+propResource+"': "+e);
			return false;
		}
	}
	
	static boolean loadFile(Properties p, String propFilename) {
		return loadFile(p, propFilename, false);
	}
	
	static boolean loadFile(Properties p, String propFilename, boolean defaultFile) {
		File propFile = new File(propFilename);
		File propFileDir = propFile.getAbsoluteFile().getParentFile();
		log.debug("propfile base dir: "+propFileDir);
		p.setProperty(PROP_PROPFILEBASEDIR, propFileDir.toString());
		
		try {
			InputStream propIS = new FileInputStream(propFile);
			p.load(propIS);
			propIS.close();
			log.info("loaded "+(defaultFile?"default ":"")+"properties file: "+propFile);
			return true;
		}
		catch(FileNotFoundException e) {
			String message = (defaultFile?"default ":"")+"properties file '"+propFile+"' not found: "+e.getMessage();
			if(defaultFile) {
				log.info(message);
			}
			else {
				log.warn(message);
			}
			return false;
		}
		catch(IOException e) {
			log.warn("error loading "+(defaultFile?"default ":"")+"file '"+propFile+"': "+e);
			return false;
		}
	}

}

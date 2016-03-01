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
				log.warn("unrecognized param '"+arg+"' (ignored). for more information use '"+PARAM_HELP+"' argument");
				//XXX: show help and exit?
			}
		}
		}
		if(loadedCount==0) {
			loadFile(papp, defaultPropFile, true);
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
						+ "\nmore info at <https://bitbucket.org/tbrugz/sqldump>\n";
					System.out.println(out);
					return true;
				}
			}
		}
		return false;
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
		loadFile(p, propFilename, false);
	}
	
	static void loadFile(Properties p, String propFilename, boolean defaultFile) {
		File propFile = new File(propFilename);
		File propFileDir = propFile.getAbsoluteFile().getParentFile();
		log.debug("propfile base dir: "+propFileDir);
		p.setProperty(PROP_PROPFILEBASEDIR, propFileDir.toString());
		
		try {
			log.info("loading "+(defaultFile?"default ":"")+"properties file: "+propFile);
			InputStream propIS = new FileInputStream(propFile);
			p.load(propIS);
			propIS.close();
		}
		catch(FileNotFoundException e) {
			log.warn((defaultFile?"default ":"")+"properties file '"+propFile+"' not found: "+e.getMessage());
		}
		catch(IOException e) {
			log.warn("error loading "+(defaultFile?"default ":"")+"file '"+propFile+"': "+e);
		}
	}

}

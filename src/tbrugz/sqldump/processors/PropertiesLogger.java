package tbrugz.sqldump.processors;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.def.AbstractProcessor;
import tbrugz.sqldump.util.Utils;

public class PropertiesLogger extends AbstractProcessor {

	static final Log log = LogFactory.getLog(PropertiesLogger.class);
	
	//prefix
	static final String PROP_PREFIX = "sqldump.proplogger";
	
	//props
	static final String PROP_PROPERTIES = PROP_PREFIX+".properties";
	
	List<String> propKeys = new ArrayList<String>();
	
	@Override
	public void setProperties(Properties prop) {
		super.setProperties(prop);
		propKeys = Utils.getStringListFromProp(prop, PROP_PROPERTIES, ",");
		if(propKeys==null) {
			log.warn("no properties to log (prop '"+PROP_PROPERTIES+"')");
		}
	}
	
	@Override
	public void process() {
		if(propKeys==null) { return; }
		for(String key: propKeys) {
			log.info(key+": "+prop.getProperty(key));
		}
	}

}

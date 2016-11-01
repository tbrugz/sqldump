package tbrugz.sqldump.datadump;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import tbrugz.sqldump.dbmodel.Constraint;
import tbrugz.sqldump.dbmodel.FK;
import tbrugz.sqldump.util.Utils;

/*
 * http://www.w3.org/TR/rdb-direct-mapping/
 * 
 * RDF/XML: add <?xml-stylesheet ?> ?
 */
public abstract class RDFAbstractSyntax extends AbstractDumpSyntax {
	
	public static final String PROP_RDF_BASE = "sqldump.rdf.base";

	static Map<String, String> namespaces = new HashMap<String, String>();
	static DateFormat xsdDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm");
	
	List<FK> fks = null;
	List<Constraint> uks = null;
	
	static {
		//load properties
		Properties prop = new Properties();
		try {
			prop.load(RDFAbstractSyntax.class.getResourceAsStream("/dumpsyntax-rdf.properties"));
		} catch (IOException e) {
			e.printStackTrace();
		}
		List<String> keys = Utils.getKeysStartingWith(prop, "prefix.");
		for(String k: keys) {
			namespaces.put(k.substring(7), prop.getProperty(k));
		}
	}
	
	@Override
	public boolean usesImportedFKs() {
		return true;
	}
	
	@Override
	public boolean usesAllUKs() {
		return true;
	}
	
	@Override
	public void setImportedFKs(List<FK> fks) {
		this.fks = fks;
	}
	
	@Override
	public void setAllUKs(List<Constraint> uks) {
		this.uks = uks;
	}
	
	public static String getLiteralValue(Object elem, Class<?> type) {
		if(elem == null) {
			return null;
		}
		else if(String.class.isAssignableFrom(type)) {
			elem = ((String) elem).replaceAll(DataDumpUtils.DOUBLEQUOTE, "&quot;");
			return DataDumpUtils.DOUBLEQUOTE+elem+DataDumpUtils.DOUBLEQUOTE;
		}
		else if(Date.class.isAssignableFrom(type)) {
			//"2010-08-30T01:33"^^xsd:dateTime
			return DataDumpUtils.DOUBLEQUOTE+xsdDateFormat.format((Date)elem)+DataDumpUtils.DOUBLEQUOTE+"^^xsd:dateTime";
		}

		return String.valueOf(elem);
	}

}

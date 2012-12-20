package tbrugz.sqldump.datadump;

import java.io.IOException;
import java.io.Writer;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import tbrugz.sqldump.SQLUtils;
import tbrugz.sqldump.dbmodel.FK;
import tbrugz.sqldump.util.Utils;

/**
 * RDB2RDF Direct Mapping Turtle dump syntax
 * 
 * @see http://en.wikipedia.org/wiki/Turtle_(syntax)
 */
public class Turtle extends RDFAbstractSyntax {

	public static String[] NAMESPACE_PREFIXES = { "rdf", "xsd" };
	public static String PROP_KEYCOLSEPARATOR = "sqldump.datadump.turtle.keycolseparator"; 
	public static String PROP_KEYINCLUDESCOLNAME = "sqldump.datadump.turtle.keyincludescolname"; 
	
	String baseUrl = null;
	String keyColSeparator = ";";
	boolean keyIncludesColName = true;

	@Override
	public void procProperties(Properties prop) {
		//baseUrl = "http://example.com/";
		baseUrl = prop.getProperty(RDFAbstractSyntax.PROP_RDF_BASE);
		keyColSeparator = prop.getProperty(PROP_KEYCOLSEPARATOR, keyColSeparator);
		keyIncludesColName = Utils.getPropBool(prop, PROP_KEYINCLUDESCOLNAME, keyIncludesColName);
	}

	@Override
	public String getSyntaxId() {
		return "turtle";
	}

	@Override
	public String getMimeType() {
		return "text/turtle";
	}
	
	@Override
	public String getDefaultFileExtension() {
		return "ttl";
	}
	
	@Override
	public void dumpHeader(Writer fos) throws IOException {
		if(baseUrl!=null) {
			fos.write("@base <"+baseUrl+"> .\n");
		}
		List<String> namespacePrefixes = Arrays.asList(NAMESPACE_PREFIXES); 
		for(String prefix: namespaces.keySet()) {
			if(namespacePrefixes.contains(prefix)) {
				fos.write("@prefix "+prefix+": <"+namespaces.get(prefix)+"> .\n");
			}
		}
		fos.write("\n");
	}
	
	@Override
	public void dumpFooter(Writer fos) throws IOException {
	}

	@Override
	public void dumpRow(ResultSet rs, long count, Writer fos)
			throws IOException, SQLException {
		String entityId = null; 
		String pkKey = getPKKey(rs);

		if(pkKey==null) {
			entityId = "_:"+count;
		}
		else {
			entityId = "<"+tableName+"/"+pkKey+">";
		}
		
		//ref:type
		fos.write(entityId+" rdf:type <"+tableName+"> .\n");
		
		//values
		List<?> vals = SQLUtils.getRowObjectListFromRS(rs, lsColTypes, numCol);
		for(int i=0;i<lsColTypes.size();i++) {
			//vals.get(i), lsColTypes.get(i)
			Object value = vals.get(i);
			if(value==null) { continue; }
			
			fos.write(entityId+" <"+
					tableName+"#"+lsColNames.get(i)+"> "+
					RDFAbstractSyntax.getLiteralValue(value, lsColTypes.get(i))+" .\n");
		}

		//FKs
		if(fks!=null) {
			for(FK fk: fks) {
				//<People/ID=7> <People#ref-deptName;deptCity> <Department/ID=23> .
				List<String> fkcols = fk.getFkColumns();
				String fkKey = getKey(rs, fkcols);
				if(fkKey==null) { continue; }
				
				String fkRef = Utils.join(fkcols, ";");
				
				fos.write(entityId+" <"+
						tableName+"#ref-"+fkRef+"> "+
						"<"+fk.getPkTable()+"/"+fkKey+"> .\n");
			}
		}
			
		fos.write("\n");
	}
	
	String getPKKey(ResultSet rs) throws SQLException {
		return getKey(rs, pkCols);
	}
	
	String getKey(ResultSet rs, List<String> cols) throws SQLException {
		if(cols==null) return null;
		
		StringBuilder sb = new StringBuilder();
		boolean isFirst = true;
		for(String col: cols) {
			if(isFirst) {
				isFirst = false;
			}
			else {
				sb.append(keyColSeparator);
			}
			String value = rs.getString(col);
			if(value==null) { return null; }
			
			if(keyIncludesColName) { sb.append(col+"="); }
			sb.append( value );
		}
		return sb.toString();
	}
}

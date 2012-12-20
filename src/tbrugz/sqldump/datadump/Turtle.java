package tbrugz.sqldump.datadump;

import java.io.IOException;
import java.io.Writer;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import tbrugz.sqldump.SQLUtils;

/**
 * RDB2RDF Direct Mapping Turtle dump syntax
 * 
 * @see http://en.wikipedia.org/wiki/Turtle_(syntax)
 */
public class Turtle extends RDFAbstractSyntax {

	public static String[] NAMESPACE_PREFIXES = { "rdf", "xsd" }; 
	String baseUrl = null;

	@Override
	public void procProperties(Properties prop) {
		// TODO Auto-generated method stub

		// TODO set base url (by property?)
		baseUrl = "http://example.com/";
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
		//XXX if has no pk?
		String pkKey = getPKKey(rs);
		
		//ref:type
		fos.write("<"+tableName+"/"+pkKey+"> rdf:type <"+tableName+"> .\n");
		
		//values
		List<?> vals = SQLUtils.getRowObjectListFromRS(rs, lsColTypes, numCol);
		for(int i=0;i<lsColTypes.size();i++) {
			//vals.get(i), lsColTypes.get(i)
			Object value = vals.get(i);
			if(value==null) { continue; }
			
			fos.write("<"+tableName+"/"+pkKey+"> <"+
					tableName+"#"+lsColNames.get(i)+"> "+
					RDFAbstractSyntax.getLiteralValue(value, lsColTypes.get(i))+" .\n");
		}
		fos.write("\n");
		
		//TODO FKs
	}
	
	String getPKKey(ResultSet rs) throws SQLException {
		StringBuilder sb = new StringBuilder();
		boolean isFirst = true;
		for(String col: pkCols) {
			if(isFirst) {
				isFirst = false;
			}
			else {
				sb.append(";");
			}
			sb.append( col+"="+rs.getString(col));
		}
		return sb.toString();
	}
}

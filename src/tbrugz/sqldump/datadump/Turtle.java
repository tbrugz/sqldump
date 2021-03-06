package tbrugz.sqldump.datadump;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.net.URLEncoder;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import tbrugz.sqldump.dbmodel.Constraint;
import tbrugz.sqldump.dbmodel.FK;
import tbrugz.sqldump.util.SQLUtils;
import tbrugz.sqldump.util.Utils;

/**
 * RDB2RDF Direct Mapping Turtle dump syntax
 * 
 * @see <a href="http://en.wikipedia.org/wiki/Turtle_(syntax)">Turtle_(syntax)</a>
 */
/*
 * TODOne if have +1 PK/UK: use owl:sameAs ?
 *  - XXX what if table has UK but no PK ?
 * TODO add 'turtle.html' syntax?
 * http://answers.semanticweb.com/questions/356/seealso-or-sameas
 */
public class Turtle extends RDFAbstractSyntax implements DumpSyntaxBuilder, Cloneable {

	static final String[] NAMESPACE_PREFIXES = { "rdf", "xsd" };
	public static final String PROP_KEY_COLSEPARATOR = "sqldump.datadump.turtle.keycolseparator";
	public static final String PROP_KEY_INCLUDESCOLNAME = "sqldump.datadump.turtle.keyincludescolname";
	public static final String PROP_KEY_APPENDSTR = "sqldump.datadump.turtle.keyappend";
	
	String baseUrl = null;
	String keyColSeparator = ";";
	String keyAppendStr = null;
	boolean keyIncludesColName = true;
	boolean dumpOwlSameAsForUKs = true; //TODO: add prop for 'dumpOwlSameAsForUKs'

	@Override
	public void procProperties(Properties prop) {
		//baseUrl = "http://example.com/";
		baseUrl = prop.getProperty(RDFAbstractSyntax.PROP_RDF_BASE);
		keyColSeparator = prop.getProperty(PROP_KEY_COLSEPARATOR, keyColSeparator);
		keyAppendStr = prop.getProperty(PROP_KEY_APPENDSTR, keyAppendStr);
		keyIncludesColName = Utils.getPropBool(prop, PROP_KEY_INCLUDESCOLNAME, keyIncludesColName);
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
		if(dumpOwlSameAsForUKs) {
			namespacePrefixes = new ArrayList<String>(namespacePrefixes);
			namespacePrefixes.add("owl");
		}
		for(String prefix: namespaces.keySet()) {
			if(namespacePrefixes.contains(prefix)) {
				fos.write("@prefix "+prefix+": <"+namespaces.get(prefix)+"> .\n");
			}
		}
		fos.write("\n");
	}
	
	@Override
	public void dumpFooter(long count, boolean hasMoreRows, Writer fos) throws IOException {
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
		
		//owl:sameAs
		if(dumpOwlSameAsForUKs && uks!=null) {
			for(Constraint uk: uks) {
				String ukKey = getKey(rs, uk.getUniqueColumns(), uk.getUniqueColumns());
				fos.write(entityId+" owl:sameAs "+
						"<"+tableName+"/"+ukKey+"> .\n");
			}
		}
		
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
				List<String> fkFKcols = fk.getFkColumns();
				List<String> fkPKcols = fk.getPkColumns();
				String fkKey = getKey(rs, fkFKcols, fkPKcols);
				if(fkKey==null) { continue; }
				
				String fkRef = Utils.join(fkFKcols, ";");
				
				fos.write(entityId+" <"+
						tableName+"#ref-"+fkRef+"> "+
						"<"+fk.getPkTable()+"/"+fkKey+"> .\n");
			}
		}
			
		fos.write("\n");
	}
	
	String getPKKey(ResultSet rs) throws SQLException, UnsupportedEncodingException {
		return getKey(rs, pkCols, pkCols);
	}
	
	String getKey(ResultSet rs, List<String> fkCols, List<String> pkCols) throws SQLException, UnsupportedEncodingException {
		if(fkCols==null) { return null; }
		
		StringBuilder sb = new StringBuilder();
		boolean isFirst = true;
		//for(String col: fkCols) {
		for(int i=0;i<fkCols.size();i++) {
			String fkCol = fkCols.get(i);
			String pkCol = pkCols.get(i);
			if(isFirst) {
				isFirst = false;
			}
			else {
				sb.append(keyColSeparator);
			}
			String value = rs.getString(fkCol);
			if(value==null) { return null; }
			
			if(keyIncludesColName) { sb.append(pkCol+"="); }
			sb.append( URLEncoder.encode(value, DataDumpUtils.CHARSET_UTF8) );
		}
		if(keyAppendStr!=null) {
			sb.append(keyAppendStr);
		}
		return sb.toString();
	}
}

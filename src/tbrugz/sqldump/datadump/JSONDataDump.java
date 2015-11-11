package tbrugz.sqldump.datadump;

import java.io.IOException;
import java.io.Writer;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.util.SQLUtils;
import tbrugz.sqldump.util.StringDecorator;
import tbrugz.sqldump.util.Utils;

/*
 * XXX: option to use 'milliseconds in Universal Coordinated Time (UTC) since epoch' as date
 *      dateformat? maybe with JSR-310 (http://threeten.sourceforge.net/)?
 * 
 * see: http://weblogs.asp.net/bleroy/archive/2008/01/18/dates-and-json.aspx
 *      http://msdn.microsoft.com/en-us/library/bb299886.aspx 
 *      
 * TODO: option to output as hash (using pkcols)
 * TODO: add prepend/append (JSONP option) / ?callback=xxx
 * 
 * see: http://dataprotocols.org/json-table-schema/
 */
public class JSONDataDump extends DumpSyntax {

	static final Log log = LogFactory.getLog(JSONDataDump.class);

	static final String JSON_SYNTAX_ID = "json";
	static final String PREFIX_JSON = "sqldump.datadump."+JSON_SYNTAX_ID;

	static final String DEFAULT_METADATA_ELEMENT = "$metadata"; // "@metadata"? "$metadata" ?
	// see http://json-schema.org/example1.html ('$' can be used in js identifier)
	
	static final String PROP_DATA_ELEMENT = PREFIX_JSON+".data-element";
	static final String PROP_ADD_METADATA = PREFIX_JSON+".add-metadata";
	static final String PROP_METADATA_ELEMENT = PREFIX_JSON+".metadata-element";
	static final String PROP_JSONP_CALLBACK = PREFIX_JSON+".callback";
	
	static final StringDecorator doubleQuoter = new StringDecorator.StringQuoterDecorator("\"");
	
	String dataElement = null; //XXX "data" as default dataElement? "rows"?
	boolean addMetadata = false;
	String metadataElement = DEFAULT_METADATA_ELEMENT;
	String callback = null;
	
	String tableName;
	int numCol;
	List<String> lsColNames = new ArrayList<String>();
	List<Class<?>> lsColTypes = new ArrayList<Class<?>>();
	
	String padding = "";
	
	boolean usePK = false; //XXX: option to set prop usePK
	List<String> pkCols;
	
	@Override
	public void procProperties(Properties prop) {
		dateFormatter = new SimpleDateFormat("\"yyyy-MM-dd\"");
		procStandardProperties(prop);
		dataElement = prop.getProperty(PROP_DATA_ELEMENT);
		addMetadata = Utils.getPropBool(prop, PROP_ADD_METADATA, addMetadata);
		metadataElement = prop.getProperty(PROP_METADATA_ELEMENT, metadataElement);
		callback = prop.getProperty(PROP_JSONP_CALLBACK);
	}

	@Override
	public void initDump(String schema, String tableName, List<String> pkCols, ResultSetMetaData md) throws SQLException {
		this.tableName = tableName;
		numCol = md.getColumnCount();
		lsColNames.clear();
		lsColTypes.clear();
		for(int i=0;i<numCol;i++) {
			lsColNames.add(md.getColumnName(i+1));
		}
		for(int i=0;i<numCol;i++) {
			lsColTypes.add(SQLUtils.getClassFromSqlType(md.getColumnType(i+1), md.getPrecision(i+1), md.getScale(i+1)));
		}
		if(usePK) {
			this.pkCols = pkCols;
		}
		if(lsColNames.size()!=lsColTypes.size()) {
			log.warn("diff lsColNames/lsColTypes sizes: "+lsColNames.size()+" ; "+lsColTypes.size());
		}
		//if(pkCols==null) { usePK = false; } else { usePK = true; }
	}
	
	@Override
	public void dumpHeader(Writer fos) throws IOException {
		String dtElem = dataElement!=null?dataElement:tableName;
		
		if(callback!=null) {
			out(callback+"(", fos);
		}
		
		if(dtElem!=null) {
			out("{\n", fos);
			//TODOne: add metadata
			if(addMetadata) {
				StringBuilder sb = new StringBuilder();
				//sb.append("\n\t\"schema\": \""+schema+"\"");
				sb.append("\n"+padding+"\t\t\"name\": \""+tableName+"\",");
				sb.append("\n"+padding+"\t\t\"columns\": ["+Utils.join(lsColNames, ", ", doubleQuoter)+"],");
				sb.append("\n"+padding+"\t\t\"columnTypes\": ["+Utils.join(getClassesSimpleName(lsColTypes), ", ", doubleQuoter)+"]");
				//sb.append("\n\t\"dataElement\": \""+dataElement+"\"");
				
				out("\t\""+metadataElement+"\": "
						+"{"
						+sb.toString()
						+"\n"+padding+"\t},"
						, fos);
			}
			outNoPadding("\n"+padding+"\t\""+dtElem+"\": "
				+(this.pkCols!=null?"{":"[")
				+"\n", fos);
		}
		else {
			outNoPadding((this.pkCols!=null?"{":"[")
				+"\n", fos);
		}
	}

	@Override
	public void dumpRow(ResultSet rs, long count, Writer fos) throws IOException, SQLException {
		StringBuffer sb = new StringBuffer();
		sb.append("\t\t"+(count==0?"":","));
		if(this.pkCols!=null) {
			sb.append("\"");
			for(int i=0;i<pkCols.size();i++) {
				if(i>0) { sb.append("_"); }
				sb.append(rs.getString(pkCols.get(i)));
			}
			sb.append("\": ");
		}
		sb.append("{");
		
		List<Object> vals = SQLUtils.getRowObjectListFromRS(rs, lsColTypes, numCol, true);
		for(int i=0;i<lsColNames.size();i++) {
			if(ResultSet.class.isAssignableFrom(lsColTypes.get(i))) {
				ResultSet rsInt = (ResultSet) vals.get(i);
				if(rsInt==null) {
					continue;
				}
				
				out(sb.toString()+",\n",fos);
				out("\t\t\t"+"\""+lsColNames.get(i)+"\": ", fos);
				sb = new StringBuffer();
				
				JSONDataDump jsondd = new JSONDataDump();
				jsondd.padding = this.padding+"\t\t";
				jsondd.dateFormatter = this.dateFormatter;
				jsondd.floatFormatter = this.floatFormatter;
				jsondd.nullValueStr = this.nullValueStr;
				//jsondd.addMetadata = this.addMetadata; ?
				//jsondd's 'dtElem' should be null... jsondd should dump array... (?) 
				// 'callback' should not be set on inner 'jsondd'
				DataDumpUtils.dumpRS(jsondd, rsInt.getMetaData(), rsInt, null, null, fos, true);
				sb.append("\n\t\t"+padding);
			}
			else {
				
			try {
				sb.append((i==0?"":",") + " \"" + lsColNames.get(i) + "\"" + ": " + DataDumpUtils.getFormattedJSONValue( vals.get(i), lsColTypes.get(i), dateFormatter ));
			}
			catch(Exception e) {
				log.warn(lsColNames+" / "+vals+" / ex: "+e);
				sb.append((i==0?"":",") + " \"" + lsColNames.get(i) + "\"" + ": " + nullValueStr);
				//e.printStackTrace();
			}

			}
		}
		sb.append("}");
		out(sb.toString()+"\n", fos);
	}

	@Override
	public void dumpFooter(long count, Writer fos) throws IOException {
		String dtElem = dataElement!=null?dataElement:tableName;
		
		if(dtElem!=null) {
			out((usePK?"}":"\t]")+"\n"+padding+"}",fos);
		}
		else {
			out((usePK?"}":"\t]"),fos);
		}
		
		if(callback!=null) {
			out(")", fos);
		}
	}

	void out(String s, Writer pw) throws IOException {
		pw.write(padding+s);
	}

	void outNoPadding(String s, Writer pw) throws IOException {
		pw.write(s);
	}
	
	/*@Override
	public boolean shouldNotWriteBOM() {
		return true;
	}*/
	
	@Override
	public String getSyntaxId() {
		return JSON_SYNTAX_ID;
	}

	// http://stackoverflow.com/questions/477816/the-right-json-content-type / http://www.ietf.org/rfc/rfc4627.txt
	@Override
	public String getMimeType() {
		return "application/json";
	}

	// see: https://tools.ietf.org/html/rfc7159#section-8.1
	@Override
	public boolean allowWriteBOM() {
		return false;
	}
	
	static final Collection<String> getClassesSimpleName(Collection<Class<?>> s) {
		Collection<String> ret = new ArrayList<String>();
		if(s==null) { return null; }
		Iterator<Class<?>> iter = s.iterator();
		while (iter.hasNext()) {
			ret.add(iter.next().getSimpleName());
		}
		return ret;
	}
}

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

import tbrugz.sqldump.resultset.ResultSetArrayAdapter;
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
public class JSONDataDump extends AbstractDumpSyntax implements DumpSyntaxBuilder, HierarchicalDumpSyntax, Cloneable {

	static final Log log = LogFactory.getLog(JSONDataDump.class);

	static final String JSON_SYNTAX_ID = "json";
	static final String PREFIX_JSON = "sqldump.datadump."+JSON_SYNTAX_ID;

	static final String DEFAULT_DATA_ELEMENT = "data";
	static final String DEFAULT_METADATA_ELEMENT = "$metadata"; // "@metadata"? "$metadata" ?
	// see http://json-schema.org/example1.html ('$' can be used in js identifier)
	static final String DEFAULT_DATE_FORMAT = "\"yyyy-MM-dd\"";
	static final boolean DEFAULT_METADATA_ADD = false;
	static final boolean DEFAULT_TABLE_AS_DATA_ELEMENT = true;
	
	static final String PROP_DATA_ELEMENT = PREFIX_JSON+".data-element";
	static final String PROP_ADD_METADATA = PREFIX_JSON+".add-metadata";
	static final String PROP_METADATA_ELEMENT = PREFIX_JSON+".metadata-element";
	static final String PROP_JSONP_CALLBACK = PREFIX_JSON+".callback";
	static final String PROP_TABLE_AS_DATA_ELEMENT = PREFIX_JSON+".table-as-data-element";

	static final String PROP_INNER_TABLE_ADD_DATA_ELEMENT = PREFIX_JSON+".inner-table.add-data-element";
	static final String PROP_INNER_TABLE_ADD_METADATA = PREFIX_JSON+".inner-table.add-metadata";
	static final String PROP_INNER_ARRAY_DUMP_AS_ARRAY = PREFIX_JSON+".inner-array-dump-as-array";
	
	static final StringDecorator doubleQuoter = new StringDecorator.StringQuoterDecorator("\"");
	
	protected boolean tableNameAsDataElement = DEFAULT_TABLE_AS_DATA_ELEMENT;
	protected String dataElement = null; //XXX "data" as default dataElement? "rows"?
	protected boolean addMetadata = false;
	protected String metadataElement = DEFAULT_METADATA_ELEMENT;
	protected String callback = null;
	
	protected boolean innerTableAddDataElement = false;
	protected boolean innerTableAddMetadata = addMetadata;
	protected boolean innerArrayDumpAsArray = false;
	
	protected boolean innerTable = false;
	protected String padding = "";
	
	protected boolean usePK = false; //XXX: option to set prop usePK
	protected boolean encloseRowWithCurlyBraquets = true;
	
	@Override
	public void procProperties(Properties prop) {
		procStandardProperties(prop);
		if(dateFormatter==null) {
			dateFormatter = new SimpleDateFormat(DEFAULT_DATE_FORMAT);
		}
		dataElement = prop.getProperty(PROP_DATA_ELEMENT);
		if(dataElement!=null) {
			tableNameAsDataElement = false;
		}
		else {
			dataElement = DEFAULT_DATA_ELEMENT;
		}
		tableNameAsDataElement = Utils.getPropBool(prop, PROP_TABLE_AS_DATA_ELEMENT, tableNameAsDataElement);
		addMetadata = Utils.getPropBool(prop, PROP_ADD_METADATA, DEFAULT_METADATA_ADD);
		metadataElement = prop.getProperty(PROP_METADATA_ELEMENT, DEFAULT_METADATA_ELEMENT);
		callback = prop.getProperty(PROP_JSONP_CALLBACK);
		innerTableAddDataElement = Utils.getPropBool(prop, PROP_INNER_TABLE_ADD_DATA_ELEMENT, innerTableAddDataElement);
		innerTableAddMetadata = Utils.getPropBool(prop, PROP_INNER_TABLE_ADD_METADATA, innerTableAddMetadata);
		innerArrayDumpAsArray = Utils.getPropBool(prop, PROP_INNER_ARRAY_DUMP_AS_ARRAY, innerArrayDumpAsArray);
		
		if(innerTableAddMetadata && !innerTableAddDataElement) {
			log.warn("[innerTableAddMetadata=="+innerTableAddMetadata+"] but [innerTableAddDataElement=="+innerTableAddDataElement+"]...");
			innerTableAddMetadata = false;
		}
		postProcProperties();
	}

	@Override
	public void initDump(String schema, String tableName, List<String> pkCols, ResultSetMetaData md) throws SQLException {
		super.initDump(schema, tableName, pkCols, md);
		if(lsColNames.size()!=lsColTypes.size()) {
			log.warn("diff lsColNames/lsColTypes sizes: "+lsColNames.size()+" ; "+lsColTypes.size());
		}
		//if(pkCols==null) { usePK = false; } else { usePK = true; }
	}
	
	protected String getDataElement() {
		return tableNameAsDataElement ? tableName : dataElement;
	}
	
	@Override
	public void dumpHeader(Writer fos) throws IOException {
		String dtElem = getDataElement();
		//String dtElem = (dataElement!=null && !dataElement.isEmpty()) ? dataElement : tableName;
		//log.debug("json: dump header: "+dtElem+" / "+tableName+" / inner="+innerTable+" / "+addMetadata+" / "+innerTableAddDataElement+" / "+innerTableAddMetadata);
		if(callback!=null) {
			out(callback+"(", fos);
		}
		
		if((!innerTable && dtElem!=null) || (innerTable && innerTableAddDataElement)) {
			outNoPadding("\n", fos);
			out("{", fos);
			if((!innerTable && addMetadata) || (innerTable && innerTableAddMetadata)) {
				StringBuilder sb = new StringBuilder();
				if(schemaName!=null) {
					sb.append("\n"+padding+"\t\t\"schema\": \""+schemaName+"\",");
				}
				sb.append("\n"+padding+"\t\t\"name\": \""+tableName+"\",");
				sb.append("\n"+padding+"\t\t\"columns\": ["+Utils.join(lsColNames, ", ", doubleQuoter)+"],");
				sb.append("\n"+padding+"\t\t\"columnTypes\": ["+Utils.join(getClassesSimpleName(lsColTypes), ", ", doubleQuoter)+"],");
				sb.append("\n"+padding+"\t\t\"dataElement\": \""+dtElem+"\"");
				
				out("\n\t"+padding+"\""+metadataElement+"\": "
						+"{"
						+sb.toString()
						+"\n"+padding+"\t},"
						, fos);
			}
			dumpXtraHeader(fos);
			outNoPadding("\n"+padding+"\t\""+dtElem+"\": "
				+(usePK?"{":"[")
				+"\n", fos);
		}
		else {
			dumpXtraHeader(fos);
			outNoPadding((usePK?"{":"[")
				+"\n", fos);
		}
	}
	
	protected void dumpXtraHeader(Writer fos) throws IOException {
	}

	@Override
	public void dumpRow(ResultSet rs, long count, Writer fos) throws IOException, SQLException {
		StringBuilder sb = new StringBuilder();
		sb.append("\t\t"+(count==0?"":","));
		if(usePK) {
			sb.append("\"");
			for(int i=0;i<pkCols.size();i++) {
				if(i>0) { sb.append("_"); }
				sb.append(rs.getString(pkCols.get(i)));
			}
			sb.append("\": ");
		}
		
		boolean dumpInnerAsArray = dumpInnerAsArray();
		if(encloseRowWithCurlyBraquets && !dumpInnerAsArray) {
			sb.append("{");
		}
		
		List<Object> vals = SQLUtils.getRowObjectListFromRS(rs, lsColTypes, numCol, true);
		for(int i=0;i<lsColNames.size();i++) {
			Object origVal = vals.get(i);
			Class<?> ctype = lsColTypes.get(i);
			boolean isResultSet = DataDumpUtils.isResultSet(ctype, origVal);
			boolean isArray = DataDumpUtils.isArray(ctype, origVal);
			sb.append((i==0?" ":", "));
			if(!dumpInnerAsArray) {
				sb.append("\"" + lsColNames.get(i) + "\"" + ": ");
			}
			
			if(isResultSet || isArray) {
				String innerTableName = lsColNames.get(i);
				ResultSet rsInt = null;
				if(isArray) {
					Object[] objArr = (Object[]) origVal;
					rsInt = new ResultSetArrayAdapter(objArr, false, innerTableName);
				}
				else {
					rsInt = (ResultSet) origVal;
				}
				if(rsInt==null) {
					continue;
				}
				
				out(sb.toString(), fos);
				sb.setLength(0);
				
				JSONDataDump jsondd = innerClone();
				//jsondd.padding = this.padding+"\t\t";
				//jsondd.callback = null;
				//jsondd.tableNameAsDataElement = true;
				//jsondd's 'dtElem' should be null... jsondd should dump array... (?) 
				// 'callback' should not be set on inner 'jsondd'
				DataDumpUtils.dumpRS(jsondd, rsInt.getMetaData(), rsInt, null, innerTableName, fos, true);
				sb.append("\n\t\t"+padding);
			}
			else {
				try {
					sb.append(DataDumpUtils.getFormattedJSONValue( origVal, ctype, dateFormatter ));
				}
				catch(Exception e) {
					log.warn("dumpRow: "+lsColNames+" / "+vals+" / ex: "+e);
					sb.append(nullValueStr);
					//e.printStackTrace();
				}
				if(i==lsColNames.size()-1) {
					sb.append(" ");
				}
			}
		}
		if(encloseRowWithCurlyBraquets && !dumpInnerAsArray) {
			sb.append("}");
		}
		sb.append("\n");
		out(sb.toString(), fos);
	}

	@Override
	public void dumpFooter(long count, boolean hasMoreRows, Writer fos) throws IOException {
		//String dtElem = dataElement!=null?dataElement:tableName;
		String dtElem = getDataElement();
		
		if((!innerTable && dtElem!=null) || (innerTable && innerTableAddDataElement)) {
			out((usePK?"\t}":"\t]")+"\n"+padding+"}",fos);
		}
		else {
			out((usePK?"\t}":"\t]"),fos);
		}
		
		if(callback!=null) {
			out(")", fos);
		}
	}

	protected void out(String s, Writer pw) throws IOException {
		pw.write(padding+s);
	}

	protected void outNoPadding(String s, Writer pw) throws IOException {
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

	@Override
	public JSONDataDump clone() throws CloneNotSupportedException {
		JSONDataDump jsondd = (JSONDataDump) super.clone();
		
		// from DumpSyntax
		updateProperties(jsondd);
		
		// from JSON
		jsondd.addMetadata = this.addMetadata;
		jsondd.callback = this.callback;
		jsondd.dataElement = this.dataElement;
		jsondd.metadataElement = this.metadataElement;
		jsondd.padding = this.padding;
		jsondd.tableNameAsDataElement = this.tableNameAsDataElement;
		
		return jsondd;
	}

	/*@Override
	public JSONDataDump build(String schemaName, String tableName,
			List<String> pkCols, ResultSetMetaData md) throws SQLException {
		JSONDataDump jsondd = clone();
		jsondd.initDump(schemaName, tableName, pkCols, md);
		return jsondd;
	}*/
	
	@Override
	public JSONDataDump innerClone() {
		try {
			JSONDataDump jsondd = clone();
			jsondd.padding = this.padding+"\t\t";
			jsondd.callback = null;
			jsondd.innerTable = true;
			return jsondd;
		} catch (CloneNotSupportedException e) {
			throw new RuntimeException(e);
		}
	}
	
	boolean dumpInnerAsArray() {
		return innerTable  && innerArrayDumpAsArray && lsColNames.size()==1;
	}

}

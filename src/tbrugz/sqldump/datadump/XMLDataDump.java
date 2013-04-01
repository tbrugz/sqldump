package tbrugz.sqldump.datadump;

import java.io.IOException;
import java.io.Writer;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.util.SQLUtils;

public class XMLDataDump extends DumpSyntax {
	
	static Log log = LogFactory.getLog(XMLDataDump.class);

	static final String XML_SYNTAX_ID = "xml";
	
	String tableName;
	int numCol;
	List<String> lsColNames = new ArrayList<String>();
	List<Class<?>> lsColTypes = new ArrayList<Class<?>>();
	
	String padding = "";
	
	@Override
	public void procProperties(Properties prop) {
		procStandardProperties(prop);
	}

	@Override
	public void initDump(String tableName, List<String> pkCols, ResultSetMetaData md) throws SQLException {
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
	}
	
	@Override
	public void dumpHeader(Writer fos) throws IOException {
		//XXX: add xml declaration? optional? e.g.: <?xml version="1.0" encoding="UTF-8" ?>
		//see http://www.w3.org/TR/REC-xml/#NT-XMLDecl
		//    http://www.ibm.com/developerworks/xml/library/x-tipdecl/index.html
		out("<"+tableName+">\n", fos);
	}

	@Override
	public void dumpRow(ResultSet rs, long count, Writer fos) throws IOException, SQLException {
		StringBuffer sb = new StringBuffer();
		//XXX: option to define 'row' xml-element
		sb.append("\t"+"<row>");
		List<Object> vals = SQLUtils.getRowObjectListFromRS(rs, lsColTypes, numCol, true);
		for(int i=0;i<lsColNames.size();i++) {
			//XXX: prop for selecting ResultSet dumping or not?
			if(ResultSet.class.isAssignableFrom(lsColTypes.get(i))) {
				ResultSet rsInt = (ResultSet) vals.get(i);
				if(rsInt==null) {
					continue;
				}
				
				out(sb.toString()+"\n", fos);
				sb = new StringBuffer();
				
				XMLDataDump xmldd = new XMLDataDump();
				xmldd.padding = this.padding+"\t\t";
				DataDumpUtils.dumpRS(xmldd, rsInt.getMetaData(), rsInt, lsColNames.get(i), fos, true);
				sb.append("\n\t");
			}
			else {
				Object value = DataDumpUtils.getFormattedXMLValue(vals.get(i), lsColTypes.get(i), floatFormatter, nullValueStr);
				//Object value = getValueNotNull( vals.get(i) );
				sb.append( "<"+lsColNames.get(i)+">"+ value +"</"+lsColNames.get(i)+">");
			}
		}
		sb.append("</row>");
		out(sb.toString()+"\n", fos);
	}

	@Override
	public void dumpFooter(Writer fos) throws IOException {
		out("</"+tableName+">", fos);
	}

	void out(String s, Writer pw) throws IOException {
		pw.write(padding+s);
	}
	
	@Override
	public String getSyntaxId() {
		return XML_SYNTAX_ID;
	}

	// http://annevankesteren.nl/2004/08/mime-types
	@Override
	public String getMimeType() {
		return "application/xml";
	}
}

package tbrugz.sqldump.datadump;

import java.io.IOException;
import java.io.Writer;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;

import tbrugz.sqldump.SQLUtils;
import tbrugz.sqldump.Utils;

/*
 * XXXdone: left-align for strings & right-align for numbers
 * XXXdone: prop for 'separator' & 'lineGroupSize'
 * XXXdone: prop for null value? defalut <null>?
 * XXXdone: prop for showing or not column names
 * 
 * XXX: problem with 'partitionby' and column of partition is null
 */
/**
 * FFC: Formatted Fixed Column
 */
public class FFCDataDump extends DumpSyntax {

	static final String PROP_DATADUMP_FFC_COLUMNDELIMITER = "sqldump.datadump.ffc.columndelimiter";
	static final String PROP_DATADUMP_FFC_LINEGROUPSIZE = "sqldump.datadump.ffc.linegroupsize";
	static final String PROP_DATADUMP_FFC_SHOWCOLNAMES = "sqldump.datadump.ffc.showcolnames";
	static final String PROP_DATADUMP_FFC_SHOWCOLNAMESLINES = "sqldump.datadump.ffc.showcolnameslines";

	static final String FFC_SYNTAX_ID = "ffc";
	//static final String DEFAULT_NULL_VALUE = "";
	static Logger log = Logger.getLogger(FFCDataDump.class);
	
	int numCol;
	List<String> lsColNames = new ArrayList<String>();
	List<Class> lsColTypes = new ArrayList<Class>();
	boolean showColNames = true, showColNamesLines = true;
	
	@Override
	public void procProperties(Properties prop) {
		procStandardProperties(prop);
		Long propLineGroupSize = Utils.getPropLong(prop, PROP_DATADUMP_FFC_LINEGROUPSIZE);
		if(propLineGroupSize!=null && propLineGroupSize>0) {
			lineGroupSize = (int)(long) propLineGroupSize;
		}
		String propColDelim = prop.getProperty(PROP_DATADUMP_FFC_COLUMNDELIMITER);
		if(propColDelim!=null) {
			separator = propColDelim;
		}
		/*String propNullValue = prop.getProperty(PROP_DATADUMP_FFC_NULLVALUE);
		if(propNullValue!=null) {
			nullValue = propNullValue;
		}*/
		showColNames = Utils.getPropBool(prop, PROP_DATADUMP_FFC_SHOWCOLNAMES, true);
		showColNamesLines = Utils.getPropBool(prop, PROP_DATADUMP_FFC_SHOWCOLNAMESLINES, true);
	}

	@Override
	public void initDump(String tableName, ResultSetMetaData md) throws SQLException {
		numCol = md.getColumnCount();		
		lsColNames.clear();
		lsColTypes.clear();
		for(int i=0;i<numCol;i++) {
			lsColNames.add(md.getColumnName(i+1));
			lsColTypes.add(SQLUtils.getClassFromSqlType(md.getColumnType(i+1), md.getScale(i+1)));
			
			if(Number.class.isAssignableFrom(lsColTypes.get(i))) {
				leftAlignField.add(false);
			}
			else {
				leftAlignField.add(true);
			}
		}
		//for(int i=0;i<lsColNames.size();i++) {
		//	log.debug("col: "+lsColNames.get(i)+"/"+lsColNames.get(i).length());
		//	headersColsMaxLenght.add(lsColNames.get(i).length());
		//}
		clearBuffer();
		//colsMaxLenght.addAll(headersColsMaxLenght);
	}
	
	int lineGroupSize = 20;
	String separator = " | ";
	//String nullValue = DEFAULT_NULL_VALUE;

	//List<Integer> headersColsMaxLenght = new ArrayList<Integer>();
	List<Integer> colsMaxLenght = new ArrayList<Integer>();
	List<List<String>> valuesBuffer = new ArrayList<List<String>>();
	List<Boolean> leftAlignField = new ArrayList<Boolean>();
	
	@Override
	public void dumpHeader(Writer fos) throws Exception {
		setColMaxLenghtForColNames();
	}
	
	void setColMaxLenghtForColNames() {
		//setting colsMaxLenght
		for(int i=0;i<lsColNames.size();i++) {
			int max = colsMaxLenght.get(i);

			if(showColNames) {
				int maxCol = lsColNames.get(i).length();
				if(max<maxCol) {
					max = maxCol;
					colsMaxLenght.set(i, max);
				}
			}
			if(colsMaxLenght.get(i)<=0) { log.warn("FFC: size=0; i="+i+"; name="+lsColNames.get(i)); }
		}
	}

	@Override
	public void dumpRow(ResultSet rs, long count, Writer fos) throws Exception {
		//first count is equal 0
		if(count%lineGroupSize==0) {
			dumpBuffer(fos);
		}

		List vals = SQLUtils.getRowObjectListFromRS(rs, lsColTypes, numCol);
		List<String> valsStr = new ArrayList<String>();
		for(int i=0;i<lsColNames.size();i++) {
			int max = colsMaxLenght.get(i);

			String valueStr = getFormattedValue(vals.get(i));
			if(max<valueStr.length()) {
				max = valueStr.length();
				colsMaxLenght.set(i, max);
			}

			if(showColNames) {
				int maxCol = lsColNames.get(i).length();
				if(max<maxCol) {
					max = maxCol;
					colsMaxLenght.set(i, max);
				}
			}
			if(colsMaxLenght.get(i)<=0) { log.warn("FFC: size=0; i="+i+"; name="+lsColNames.get(i)); }
			valsStr.add(valueStr);
		}
		valuesBuffer.add(valsStr);
	}
	
	void clearBuffer() {
		//clean up
		valuesBuffer.clear();
		colsMaxLenght.clear();
		//colsMaxLenght.addAll(headersColsMaxLenght);
		for(int i=0;i<lsColNames.size();i++) { colsMaxLenght.add(0); }
	}
	
	void dumpBuffer(Writer fos) throws IOException {
		if(valuesBuffer.size()<=0) { return; } //should it be here? XXX: dump header only when rowCount = 0? 
		
		//print buffer
		StringBuffer sb = new StringBuffer();

		if(showColNames) {
			if(showColNamesLines) {
				//upper line
				for(int j=0;j<lsColNames.size();j++) {
					//log.debug("format: "+colsMaxLenght.get(j)+": "+lsColNames.get(j)+"/"+lsColNames.get(j).length());
					appendPattern(sb, colsMaxLenght.get(j), "-", "-+-");
				}
				sb.append("\n");
			}
	
			//col names
			for(int j=0;j<lsColNames.size();j++) {
				//log.debug("format: "+colsMaxLenght.get(j)+": "+lsColNames.get(j)+"/"+lsColNames.get(j).length());
				appendString(sb, colsMaxLenght.get(j), lsColNames.get(j), j);
			}
			sb.append("\n");
	
			if(showColNamesLines) {
				//lower line
				for(int j=0;j<lsColNames.size();j++) {
					//log.debug("format: "+colsMaxLenght.get(j)+": "+lsColNames.get(j)+"/"+lsColNames.get(j).length());
					appendPattern(sb, colsMaxLenght.get(j), "-", "-+-");
				}
				sb.append("\n");
			}
		}
		
		for(int i=0;i<valuesBuffer.size();i++) {
			List<String> vals = valuesBuffer.get(i);
			for(int j=0;j<lsColNames.size();j++) {
				appendString(sb, colsMaxLenght.get(j), vals.get(j), j );
			}
			sb.append("\n");
		}
		out(sb.toString(), fos); //+"\n"
		
		clearBuffer();
	}
	
	void appendString(StringBuffer sb, int len, String value, int colIndex) {
		if(len==0) {
			log.warn("FFCSyntax error: len="+len+"; value: "+value+"; bufsize="+valuesBuffer.size());
		}
		if(leftAlignField.get(colIndex)) {
			sb.append( String.format("%-"+len+"s"+separator, value) );
		}
		else {
			sb.append( String.format("%"+len+"s"+separator, value) );
		}
	}

	void appendPattern(StringBuffer sb, int len, String pattern, String separator) {
		//sb.append( String.format("%"+len+"s"+separator, value) );
		for(int i=0;i<len;i++) {
			sb.append( pattern );
		}
		sb.append(separator);
	}
	
	String getFormattedValue(Object o) {
		//if(o==null) return nullValue;
		return Utils.getFormattedCSVValue(o, floatFormatter, null, nullValueStr);
	}

	@Override
	public void dumpFooter(Writer fos) throws Exception {
		//setColMaxLenghtForColNames();
		dumpBuffer(fos);
	}
	
	@Override
	public void flushBuffer(Writer fos) throws Exception {
		if(valuesBuffer.size()<=0) { return; } //not needed now: dumpBuffer() already does it
		dumpBuffer(fos);
	}

	void out(String s, Writer pw) throws IOException {
		pw.write(s);
	}
	
	@Override
	public String getSyntaxId() {
		return FFC_SYNTAX_ID;
	}
	
	@Override
	public String getDefaultFileExtension() {
		return "ffc.txt";
	}
}

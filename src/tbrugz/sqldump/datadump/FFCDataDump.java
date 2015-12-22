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
import tbrugz.sqldump.util.Utils;

/*
 * XXXdone: left-align for strings & right-align for numbers
 * XXXdone: prop for 'separator' & 'lineGroupSize'
 * XXXdone: prop for null value? defalut <null>?
 * XXXdone: prop for showing or not column names
 * XXX change to FWFDataDump? R: read.fwf {utils}: Read Fixed Width Format Files
 * 
 * XXX: problem with 'partitionby' and column of partition is null
 * XXX: add prop for 'recordDemimiter' ?
 * XXX: option to clip/crop values?
 * 
 * http://stackoverflow.com/questions/7666780/why-are-fixed-width-file-formats-still-in-use
 * http://docs.aws.amazon.com/redshift/latest/dg/t_unloading_fixed_width_data.html - fixedwidth '0:3,1:100,2:30,3:2,4:6';
 * https://docs.tibco.com/pub/enterprise-runtime-for-R/4.0.0/doc/html/Language_Reference/utils/read.fwf.html
 * http://stackoverflow.com/questions/14383710/read-fixed-width-text-file
 */
/**
 * FFC: Formatted Fixed Column
 * 
 * a.k.a: FF (fixed field), FWF (fixed width format)
 * 
 * see:
 * https://stat.ethz.ch/R-manual/R-devel/library/utils/html/read.fwf.html
 * https://www.treasury.gov/resource-center/sanctions/SDN-List/Documents/dat_spec.txt
 */
public class FFCDataDump extends DumpSyntax implements Cloneable {

	static final String PROP_DATADUMP_FFC_COLUMNDELIMITER = "sqldump.datadump.ffc.columndelimiter";
	static final String PROP_DATADUMP_FFC_LINEGROUPSIZE = "sqldump.datadump.ffc.linegroupsize";
	static final String PROP_DATADUMP_FFC_SHOWCOLNAMES = "sqldump.datadump.ffc.showcolnames";
	static final String PROP_DATADUMP_FFC_SHOWCOLNAMESLINES = "sqldump.datadump.ffc.showcolnameslines";

	static final String FFC_SYNTAX_ID = "ffc";
	//static final String DEFAULT_NULL_VALUE = "";
	static final Log log = LogFactory.getLog(FFCDataDump.class);
	
	static final long DEFAULT_LINEGROUPSIZE = 20;
	
	static final String recordDemimiter = "\n";
	
	transient int numCol;
	List<String> lsColNames = new ArrayList<String>();
	List<Class<?>> lsColTypes = new ArrayList<Class<?>>();
	boolean showColNames = true, showColNamesLines = true,
			show1stColSeparator = true, mergeBlocksSeparatorLines = true,
			showTrailerLine = true, showTrailerLineAllBlocks = false;
	
	@Override
	public void procProperties(Properties prop) {
		procStandardProperties(prop);
		Long propLineGroupSize = Utils.getPropLong(prop, PROP_DATADUMP_FFC_LINEGROUPSIZE, DEFAULT_LINEGROUPSIZE);
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
	public void initDump(String schema, String tableName, List<String> pkCols, ResultSetMetaData md) throws SQLException {
		numCol = md.getColumnCount();
		lsColNames.clear();
		lsColTypes.clear();
		lastBlockLineSize = 0;
		for(int i=0;i<numCol;i++) {
			lsColNames.add(md.getColumnName(i+1));
			lsColTypes.add(SQLUtils.getClassFromSqlType(md.getColumnType(i+1), md.getPrecision(i+1), md.getScale(i+1)));
			
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
	
	int lineGroupSize = (int) DEFAULT_LINEGROUPSIZE;

	String separator = "|";
	String firstColSep = "+";
	String colNamesLineCrossSep = "+";
	String colNamesLineSep = "-";
	
	List<Boolean> leftAlignField = new ArrayList<Boolean>();
	
	//"stateful" props
	List<Integer> colsMaxLenght = new ArrayList<Integer>();
	List<List<String>> valuesBuffer = new ArrayList<List<String>>();
	int lastBlockLineSize = 0;
	boolean shouldClearBuffer = false;
	//end stateful props
	
	@Override
	public void dumpHeader(Writer fos) {
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

	static boolean resultSetWarned = false;
	
	@Override
	public void dumpRow(ResultSet rs, long count, Writer fos) throws IOException, SQLException {
		//first count is equal 0
		if(count%lineGroupSize==0) {
			dumpBuffer(fos);
			if(shouldClearBuffer) {
				clearBuffer();
				shouldClearBuffer = false;
			}
		}

		List<Object> vals = SQLUtils.getRowObjectListFromRS(rs, lsColTypes, numCol);
		List<String> valsStr = new ArrayList<String>();
		for(int i=0;i<lsColNames.size();i++) {
			int max = colsMaxLenght.get(i);

			String valueStr = null;
			if(ResultSet.class.isAssignableFrom(lsColTypes.get(i))) {
				if(!resultSetWarned) {
					log.warn("can't dump ResultSet as column");
					resultSetWarned = true;
				}
				valueStr = nullValueStr;
			}
			else {
				valueStr = getFormattedValue(vals.get(i), lsColTypes.get(i));
			}
			
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
	
	//XXX: rename to writeBuffer()
	void dumpBuffer(Writer fos) throws IOException {
		if(valuesBuffer.size()<=0) { return; } //should it be here? XXX: dump header only when rowCount = 0? 
		
		dumpColumnNames(fos);
		
		//print buffer
		StringBuilder sb = new StringBuilder();

		for(int i=0;i<valuesBuffer.size();i++) {
			if(show1stColSeparator) { sb.append(separator); }
			List<String> vals = valuesBuffer.get(i);
			for(int j=0;j<lsColNames.size();j++) {
				appendString(sb, colsMaxLenght.get(j), vals.get(j), j );
			}
			sb.append(recordDemimiter);
		}
		
		if(showTrailerLineAllBlocks) {
			appendLine(sb, false);
		}
		
		out(sb.toString(), fos); //+"\n"
		
		shouldClearBuffer = true;
		int thisBlockSize = 0;
		for(int j=0;j<lsColNames.size();j++) {
			thisBlockSize += colsMaxLenght.get(j);
		}
		
		lastBlockLineSize = thisBlockSize;
		//clearBuffer();
	}
	
	void dumpColumnNames(Writer fos) throws IOException {
		StringBuilder sb = new StringBuilder();
		if(showColNames) {
			if(showColNamesLines) {
				//upper line
				appendLine(sb, mergeBlocksSeparatorLines);
			}
	
			//col names
			if(show1stColSeparator) { sb.append(separator); }
			for(int j=0;j<lsColNames.size();j++) {
				//log.debug("format: "+colsMaxLenght.get(j)+": "+lsColNames.get(j)+"/"+lsColNames.get(j).length());
				appendString(sb, colsMaxLenght.get(j), lsColNames.get(j), j);
			}
			sb.append(recordDemimiter);
	
			if(showColNamesLines) {
				//lower line
				appendLine(sb, false);
			}
		}
		out(sb.toString(), fos); //+"\n"
	}
	
	void appendLine(StringBuilder sb, boolean isBlock1stLine) {
		if(show1stColSeparator) { sb.append(firstColSep); }
		//lower line
		int colsSize = 0;
		for(int j=0;j<lsColNames.size();j++) {
			//log.debug("format: "+colsMaxLenght.get(j)+": "+lsColNames.get(j)+"/"+lsColNames.get(j).length());
			appendPattern(sb, colsMaxLenght.get(j), colNamesLineSep, colNamesLineCrossSep);
			colsSize += colsMaxLenght.get(j);
		}
		if(isBlock1stLine) {
			if(colsSize<lastBlockLineSize) {
				appendPattern(sb, lastBlockLineSize-colsSize-1, colNamesLineSep, colNamesLineCrossSep);
			}
		}
		sb.append(recordDemimiter);
	}
	
	void appendString(StringBuilder sb, int len, String value, int colIndex) {
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

	void appendPattern(StringBuilder sb, int len, String pattern, String separator) {
		//sb.append( String.format("%"+len+"s"+separator, value) );
		for(int i=0;i<len;i++) {
			sb.append( pattern );
		}
		sb.append(separator);
	}
	
	String getFormattedValue(Object o, Class<?> c) {
		//if(o==null) return nullValue;
		return DataDumpUtils.getFormattedCSVValue(o, c, floatFormatter, dateFormatter, null, recordDemimiter, null, nullValueStr);
	}

	@Override
	public void dumpFooter(long count, Writer fos) throws IOException {
		//setColMaxLenghtForColNames();
		dumpBuffer(fos);
		if(count==0) {
			dumpColumnNames(fos);
		}
		else if(showTrailerLine && !showTrailerLineAllBlocks) {
			StringBuilder sb = new StringBuilder();
			appendLine(sb, false);
			out(sb.toString(), fos);
		}
		clearBuffer();
	}
	
	@Override
	public void flushBuffer(Writer fos) throws IOException {
		if(valuesBuffer.size()<=0) { return; } //not needed now: dumpBuffer() already does it
		shouldClearBuffer = true;
		//dumpBuffer(fos);
	}

	void out(String s, Writer pw) throws IOException {
		pw.write(s);
	}
	
	@Override
	public boolean isStateful() {
		return true;
	}
	
	@Override
	public String getSyntaxId() {
		return FFC_SYNTAX_ID;
	}
	
	@Override
	public String getDefaultFileExtension() {
		return "ffc.txt";
	}
	
	@Override
	public Object clone() throws CloneNotSupportedException {
		return super.clone();
		/*FFCDataDump newffc = new FFCDataDump();
		super.copyPropsTo(newffc);
		
		//procproperties props
		newffc.lineGroupSize = this.lineGroupSize;
		newffc.separator = this.separator;
		newffc.showColNames = this.showColNames;
		newffc.showColNamesLines = this.showColNamesLines;
		//initDump props
		newffc.numCol = this.numCol;
		newffc.lsColNames = this.lsColNames;
		newffc.lsColTypes = this.lsColTypes;
		newffc.leftAlignField = this.leftAlignField;
		//setup
		newffc.clearBuffer();
		
		return newffc;*/
	}

	@Override
	public String getMimeType() {
		return "text/plain";
	}
}

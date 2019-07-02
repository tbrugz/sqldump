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
 * 
 * XXX: word-wrap?
 * https://tools.ietf.org/html/rfc3676
 * https://en.wikipedia.org/wiki/Plain_text
 * http://stackoverflow.com/questions/5837556/how-to-disable-word-wrapping-in-plain-text-files-in-chrome
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
public class FFCDataDump extends AbstractDumpSyntax implements Cloneable, DumpSyntaxBuilder {

	static final String PROP_DATADUMP_FFC_COLUMNDELIMITER = "sqldump.datadump.ffc.columndelimiter";
	static final String PROP_DATADUMP_FFC_LINEGROUPSIZE = "sqldump.datadump.ffc.linegroupsize";
	static final String PROP_DATADUMP_FFC_SHOWCOLNAMES = "sqldump.datadump.ffc.showcolnames";
	static final String PROP_DATADUMP_FFC_SHOWCOLNAMESLINES = "sqldump.datadump.ffc.showcolnameslines";
	static final String PROP_DATADUMP_FFC_SPACES_FOR_EACH_TAB = "sqldump.datadump.ffc.spaces-for-each-tab";
	static final String PROP_DATADUMP_FFC_ALIGNED_TAB_REPLACING = "sqldump.datadump.ffc.aligned-tab-replacing";

	static final String FFC_SYNTAX_ID = "ffc";
	
	static final String PLAINTEXT_MIMETYPE = "text/plain";
	
	//static final String DEFAULT_NULL_VALUE = "";
	static final Log log = LogFactory.getLog(FFCDataDump.class);
	
	static final long DEFAULT_LINEGROUPSIZE = 20;
	
	static final String recordDemimiter = "\n";
	
	boolean showColNames = true, showColNamesUpperLine = true, show1stColNamesUpperLine = true, showColNamesLowerLine = true,
			show1stColSeparator = true, mergeBlocksSeparatorLines = true, repeatHeader = true,
			showTrailerLine = true, showTrailerLineAllBlocks = false;
	
	Integer spacesForEachTab = null;
	boolean alignedTabReplacing = true;
	
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
			firstPositionSeparator = separator;
			lastPositionSeparator = separator;
		}
		/*String propNullValue = prop.getProperty(PROP_DATADUMP_FFC_NULLVALUE);
		if(propNullValue!=null) {
			nullValue = propNullValue;
		}*/
		showColNames = Utils.getPropBool(prop, PROP_DATADUMP_FFC_SHOWCOLNAMES, true);
		showColNamesUpperLine = Utils.getPropBool(prop, PROP_DATADUMP_FFC_SHOWCOLNAMESLINES, true);
		show1stColNamesUpperLine = showColNamesUpperLine;
		showColNamesLowerLine = Utils.getPropBool(prop, PROP_DATADUMP_FFC_SHOWCOLNAMESLINES, true);
		spacesForEachTab = Utils.getPropInt(prop, PROP_DATADUMP_FFC_SPACES_FOR_EACH_TAB);
		alignedTabReplacing = Utils.getPropBool(prop, PROP_DATADUMP_FFC_ALIGNED_TAB_REPLACING, alignedTabReplacing);
		postProcProperties();
		validateProperties();
	}
	
	boolean areOfSameLength(String s1, String s2) {
		if(s1==null || s2==null) {
			return false;
		}
		if(s1.length()!=s2.length()) {
			return false;
		}
		return true;
	}

	boolean isOfLength(String s, int length) {
		return s!=null && s.length()==length;
	}
	
	void validateProperties() {
		if(!areOfSameLength(colNamesLineLastCrossSep, lastPositionSeparator)) {
			log.warn("lastColSeparators differ in length");
		}
		if(!areOfSameLength(firstColSep, firstPositionSeparator)) {
			log.warn("firstColSeparators differ in length");
		}
		if(!areOfSameLength(separator, colNamesLineCrossSep)) {
			log.warn("middleColSeparators differ in length");
		}
		
		// header
		if(!areOfSameLength(headerLine1stSep, firstColSep)) {
			log.warn("headerLine1stSep & firstColSeparator differ in length");
		}
		if(!areOfSameLength(headerLineMiddleSep, separator)) {
			log.warn("headerLineMiddleSep & separator differ in length");
		}
		if(!areOfSameLength(headerLineLastSep, lastPositionSeparator)) {
			log.warn("headerLineLastSep & lastColSeparator differ in length");
		}
		
		// footer
		if(!areOfSameLength(footerLine1stSep, firstColSep)) {
			log.warn("footerLine1stSep & firstColSeparator differ in length");
		}
		if(!areOfSameLength(footerLineMiddleSep, separator)) {
			log.warn("footerLineLastSep & separator differ in length");
		}
		if(!areOfSameLength(footerLineLastSep, lastPositionSeparator)) {
			log.warn("footerLineMiddleSep & lastColSeparator differ in length");
		}

		if(!isOfLength(colNamesLineSep, 1)) {
			log.warn("colNamesLineSep must have length == 1");
		}
	}

	@Override
	public void initDump(String schema, String tableName, List<String> pkCols, ResultSetMetaData md) throws SQLException {
		super.initDump(schema, tableName, pkCols, md);
		lastBlockLineSize = 0;
		for(int i=0;i<numCol;i++) {
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
	String firstPositionSeparator = separator;
	String lastPositionSeparator = separator;
	String firstColSep = "+";
	String colNamesLineCrossSep = "+";
	String colNamesLineSep = "-";
	String colNamesLineLastCrossSep = "+";
	
	String headerLine1stSep = "+";
	String headerLineLastSep = "+";
	String headerLineMiddleSep = "+";
	String footerLine1stSep = "+";
	String footerLineLastSep = "+";
	String footerLineMiddleSep = "+";
	
	List<Boolean> leftAlignField = new ArrayList<Boolean>();
	
	//"stateful" props
	List<Integer> colsMaxLenght = new ArrayList<Integer>();
	List<List<String>> valuesBuffer = new ArrayList<List<String>>();
	int lastBlockLineSize = 0;
	boolean shouldClearBuffer = false;
	boolean firstHeaderDumped = false;
	boolean lastLineDumped = false;
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
		
		// repeat header?
		if(repeatHeader || !firstHeaderDumped) {
			dumpColumnNames(fos);
			firstHeaderDumped = true;
		}
		
		//print buffer
		StringBuilder sb = new StringBuilder();

		for(int i=0;i<valuesBuffer.size();i++) {
			if(show1stColSeparator) { sb.append(firstPositionSeparator); }
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
			if( (showColNamesUpperLine && firstHeaderDumped) || (show1stColNamesUpperLine && !firstHeaderDumped) ) {
				//upper line
				appendLine(sb, mergeBlocksSeparatorLines);
			}
	
			//col names
			if(show1stColSeparator) { sb.append(firstPositionSeparator); }
			for(int j=0;j<lsColNames.size();j++) {
				//log.debug("format: "+colsMaxLenght.get(j)+": "+lsColNames.get(j)+"/"+lsColNames.get(j).length());
				appendString(sb, colsMaxLenght.get(j), lsColNames.get(j), j);
			}
			sb.append(recordDemimiter);
	
			if(showColNamesLowerLine) {
				//lower line
				appendColNamesLowerLine(sb, false);
			}
		}
		out(sb.toString(), fos); //+"\n"
	}
	
	void appendColNamesLowerLine(StringBuilder sb, boolean isBlock1stLine) {
		appendLine(sb, isBlock1stLine);
	}
	
	void appendLine(StringBuilder sb, boolean isBlock1stLine) {
		if(show1stColSeparator) { sb.append(firstHeaderDumped ? (lastLineDumped ? footerLine1stSep : firstColSep) : headerLine1stSep); }
		//lower line
		int colsSize = 0;
		for(int j=0;j<numCol;j++) {
			//log.debug("format: "+colsMaxLenght.get(j)+": "+lsColNames.get(j)+"/"+lsColNames.get(j).length());
			String sep = j + 1 < numCol ?
					(firstHeaderDumped ? (lastLineDumped ? footerLineMiddleSep : colNamesLineCrossSep) : headerLineMiddleSep) :
					(firstHeaderDumped ? (lastLineDumped ? footerLineLastSep : colNamesLineLastCrossSep) : headerLineLastSep);
			appendPattern(sb, colsMaxLenght.get(j), colNamesLineSep, sep);
			colsSize += colsMaxLenght.get(j);
		}
		if(isBlock1stLine) {
			if(colsSize<lastBlockLineSize-colNamesLineLastCrossSep.length()) {
				appendPattern(sb, lastBlockLineSize-colNamesLineLastCrossSep.length()-colsSize, colNamesLineSep, colNamesLineLastCrossSep);
			}
		}
		sb.append(recordDemimiter);
	}
	
	void appendString(StringBuilder sb, int len, String value, int colIndex) {
		if(len==0) {
			log.warn("FFCSyntax error: len="+len+"; value: "+value+"; bufsize="+valuesBuffer.size());
		}
		String sep = colIndex + 1 < numCol ? separator : lastPositionSeparator;
		if(leftAlignField.get(colIndex)) {
			sb.append( String.format("%-"+len+"s"+sep, value) );
		}
		else {
			sb.append( String.format("%"+len+"s"+sep, value) );
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
		String value = DataDumpUtils.getFormattedCSVValue(o, c, floatFormatter, dateFormatter, null, recordDemimiter, null, nullValueStr);
		if(spacesForEachTab!=null) {
			return replaceTabs(value);
		}
		return value;
	}
	
	@Override
	public void dumpFooter(long count, boolean hasMoreRows, Writer fos) throws IOException {
		//setColMaxLenghtForColNames();
		dumpBuffer(fos);
		lastLineDumped = true;
		//XXX lastLineDumped:: what if count==0, buffer empty on dumpFooter(), showTrailerLine && !showTrailerLineAllBlocks ...
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
	
	String replaceTabs(String value) {
		if(alignedTabReplacing) {
			return replaceTabsVariableLength(value);
		}
		else {
			return replaceTabsFixedLength(value);
		}
	}

	String replaceTabsVariableLength(String value) {
		//System.out.println("replaceTabsVariableLength: ["+value+"]");
		StringBuilder sb = new StringBuilder();
		for(int i=0;i<value.length();i++) {
			char c = value.charAt(i);
			if(c=='\t') {
				int rem = sb.length() % spacesForEachTab;
				//System.out.println("reminder["+i+"]: "+rem);
				for(int j=0;j<spacesForEachTab-rem;j++) {
					sb.append(" ");
				}
			}
			else {
				sb.append(c);
			}
		}
		//System.out.println("replaceTabsVariableLength:END: ["+sb.toString()+"]");
		return sb.toString();
	}

	String replaceTabsFixedLength(String value) {
		StringBuilder sb = new StringBuilder();
		for(int i=0;i<value.length();i++) {
			char c = value.charAt(i);
			if(c=='\t') {
				for(int j=0;j<spacesForEachTab;j++) {
					sb.append(" ");
				}
			}
			else {
				sb.append(c);
			}
		}
		return sb.toString();
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
	
	/*@Override
	public FFCDataDump clone() throws CloneNotSupportedException {
		return (FFCDataDump) super.clone();
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
		
		return newffc;* /
	}*/

	@Override
	public String getMimeType() {
		return PLAINTEXT_MIMETYPE; //XXX add ";Format=Fixed" ?
	}
}

package tbrugz.sqldump.datadump;

import java.io.IOException;
import java.io.Writer;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.resultset.pivot.PivotResultSet;
import tbrugz.sqldump.util.SQLUtils;
import tbrugz.sqldump.util.Utils;

/*
 * XXX: prop for stylesheet?
 * TODO: parameter: merge hierarchical headers (PivotRS)
 * 
 * see: https://developer.mozilla.org/en/docs/Web/HTML/Element/table
 */
public class HTMLDataDump extends XMLDataDump implements DumpSyntaxBuilder, HierarchicalDumpSyntax, Cloneable {

	static final Log log = LogFactory.getLog(HTMLDataDump.class);

	static final String HTML_SYNTAX_ID = "html";
	
	public static final String UNICODE_NULL = "&#9216"; // NULL unicode char in HTML - unicode U+2400
	
	static final String PROP_HTML_PREPEND = "sqldump.datadump.html.prepend";
	static final String PROP_HTML_APPEND = "sqldump.datadump.html.append";
	static final String PROP_HTML_ADD_CAPTION = "sqldump.datadump.html.add-caption";
	static final String PROP_HTML_STYTE_NUMERIC_ALIGN_RIGHT = "sqldump.datadump.html.style.numeric-align-right";
	static final String PROP_HTML_XPEND_INNER_TABLE = "sqldump.datadump.html.xpend-inner-table";
	static final String PROP_HTML_INNER_ARRAY_HEADER = "sqldump.datadump.html.inner-array-dump-header";
	//static final String PROP_HTML_NULLVALUE_CLASS = "sqldump.datadump.html.nullvalue-class";
	//XXX add props 'sqldump.datadump.html.inner-table.[prepend|append]' ??
	static final String PROP_HTML_BREAK_COLUMNS = "sqldump.datadump.html.break-columns";
	static final String PROP_HTML_BREAKS_ADD_COL_HEADER_BEFORE = "sqldump.datadump.html.breaks.add-col-header-before";
	static final String PROP_HTML_BREAKS_ADD_COL_HEADER_AFTER = "sqldump.datadump.html.breaks.add-col-header-after";

	public static final String PROP_PIVOT_ONROWS = "sqldump.datadump.pivot.onrows";
	public static final String PROP_PIVOT_ONCOLS = "sqldump.datadump.pivot.oncols";
	public static final String PROP_PIVOT_COLSEP = "sqldump.datadump.pivot.colsep";
	public static final String PROP_PIVOT_COLVALSEP = "sqldump.datadump.pivot.colvalsep";
	
	//protected String tableName;
	//protected int numCol;
	//protected List<String> lsColNames = new ArrayList<String>();
	//protected List<Class<?>> lsColTypes = new ArrayList<Class<?>>();
	
	protected static final String DEFAULT_PADDING = "";
	protected static final boolean DEFAULT_ADD_CAPTION = false;
	protected static final String nullPlaceholderReplacer = UNICODE_NULL; // user by pivot & breaks

	//protected String padding;
	protected boolean innerTable;
	
	protected String prepend = null;
	protected String append = null;
	//protected String nullValueClass = null;
	protected boolean dumpCaptionElement = DEFAULT_ADD_CAPTION;
	//TODO: prop for 'dumpColElement'
	protected boolean dumpColElement = false;
	protected boolean dumpStyleNumericAlignRight = false;
	protected boolean dumpColType = false; //XXX add prop for 'dumpColType'
	protected boolean dumpIsNumeric = false; //XXX add prop for 'dumpIsNumeric'
	protected boolean xpendInnerTable = true;
	protected boolean innerArrayDumpHeader = true;
	
	protected List<String> finalColNames = new ArrayList<String>();
	protected List<Class<?>> finalColTypes = new ArrayList<Class<?>>();
	
	// pivot properties
	protected PivotInfo pivotInfo = null;
	protected String colSep = null;
	protected String colValSep = null;
	protected String colSepPattern = null;
	protected String colValSepPattern = null;
	protected static final String nullPlaceholder = PivotResultSet.NULL_PLACEHOLDER;

	// break properties
	protected List<String> breakColNamesProperty = new ArrayList<String>();
	protected boolean breakColsAddColumnHeaderBeforeProperty = false;
	protected boolean breakColsAddColumnHeaderAfterProperty = false;
	
	// break state
	protected final List<Integer> breakColIndexes = new ArrayList<Integer>();
	protected final List<Object> breakColValues = new ArrayList<Object>();
	protected boolean breakColsAddColumnHeaderBefore = false;
	protected boolean breakColsAddColumnHeaderAfter = false;
	
	public HTMLDataDump() {
		//this(DEFAULT_PADDING, false);
		this.padding = DEFAULT_PADDING;
		this.innerTable = false;
	}
	
	/*public HTMLDataDump(String padding, boolean innerTable) {
		this.padding = padding;
		this.innerTable = innerTable;
	}*/
	
	@Override
	public void procProperties(Properties prop) {
		super.procProperties(prop);
		//procStandardProperties(prop);
		prepend = prop.getProperty(PROP_HTML_PREPEND);
		append = prop.getProperty(PROP_HTML_APPEND);
		//nullValueClass = prop.getProperty(PROP_HTML_NULLVALUE_CLASS);
		dumpCaptionElement = Utils.getPropBool(prop, PROP_HTML_ADD_CAPTION, DEFAULT_ADD_CAPTION);
		dumpStyleNumericAlignRight = Utils.getPropBool(prop, PROP_HTML_STYTE_NUMERIC_ALIGN_RIGHT, dumpStyleNumericAlignRight);
		xpendInnerTable = Utils.getPropBool(prop, PROP_HTML_XPEND_INNER_TABLE, xpendInnerTable);
		innerArrayDumpHeader = Utils.getPropBool(prop, PROP_HTML_INNER_ARRAY_HEADER, innerArrayDumpHeader);
		breakColNamesProperty = Utils.getStringListFromProp(prop, PROP_HTML_BREAK_COLUMNS, ",");
		//log.info("breakColNamesProperty = "+breakColNamesProperty);
		if(breakColNamesProperty!=null) {
			//XXX: breaks: [boolean properties] add column names on each break? add them before or after the break?
			breakColsAddColumnHeaderBeforeProperty = Utils.getPropBool(prop, PROP_HTML_BREAKS_ADD_COL_HEADER_BEFORE, breakColsAddColumnHeaderBeforeProperty);
			breakColsAddColumnHeaderAfterProperty = Utils.getPropBool(prop, PROP_HTML_BREAKS_ADD_COL_HEADER_AFTER, breakColsAddColumnHeaderAfterProperty);
			if(breakColsAddColumnHeaderBeforeProperty && breakColsAddColumnHeaderAfterProperty) {
				breakColsAddColumnHeaderBeforeProperty = false;
				log.warn("'"+PROP_HTML_BREAKS_ADD_COL_HEADER_BEFORE+"' and '"+PROP_HTML_BREAKS_ADD_COL_HEADER_AFTER+
						"' cant' both be true. Only 'after' will be setted");
			}
		}
		procPivotProperties(prop);
	}

	public void procPivotProperties(Properties prop) {
		int onRowsColCount = 0;
		int onColsColCount = 0;
		String onrows = prop.getProperty(PROP_PIVOT_ONROWS);
		String oncols = prop.getProperty(PROP_PIVOT_ONCOLS);
		//if(onrows!=null || oncols!=null) {
		if(onrows!=null) {
			onRowsColCount = onrows.split(",").length;
		}
		if(oncols!=null) {
			onColsColCount = oncols.split(",").length;
		}
		pivotInfo = new PivotInfo(onColsColCount, onRowsColCount);
		//}
		if(pivotInfo.isPivotResultSet()) {
			colSep = prop.getProperty(PROP_PIVOT_COLSEP, PivotResultSet.COLS_SEP);
			colValSep = prop.getProperty(PROP_PIVOT_COLVALSEP, PivotResultSet.COLVAL_SEP);
			colSepPattern = Pattern.quote(colSep);
			colValSepPattern = Pattern.quote(colValSep);
		}
	}
	
	/*
	public boolean isPivotResultSet() {
		return onRowsColCount>0 || onColsColCount>0;
	}
	*/
	
	@Override
	public void initDump(String schema, String tableName, List<String> pkCols, ResultSetMetaData md) throws SQLException {
		super.initDump(schema, tableName, pkCols, md);
		initOutputCols();
		/*if(onRowsColCount+onColsColCount>finalColNames.size()) {
			System.err.println("onRowsColCount+onColsColCount>finalColNames.size(): onRowsColCount="+onRowsColCount+" ; onColsColCount="+onColsColCount+" ; finalColNames.size()="+finalColNames.size());
		}*/
	}
	
	protected void initOutputCols() {
		finalColNames.clear();
		finalColNames.addAll(lsColNames);
		finalColTypes.clear();
		finalColTypes.addAll(lsColTypes);
		breakColIndexes.clear();
		if(breakColNamesProperty!=null) {
			for(int i=0;i<breakColNamesProperty.size();i++) {
				String breakColName = breakColNamesProperty.get(i);
				int idx = lsColNames.indexOf(breakColName);
				if(idx>=0) {
					breakColIndexes.add(idx);
					int finalIdx = finalColNames.indexOf(breakColName);
					if(finalIdx>=0) {
						String removedColName = finalColNames.remove(finalIdx);
						Class<?> removedColClazz = finalColTypes.remove(finalIdx);
						if(removedColName==null) {
							log.warn("column not found in 'finalColNames': "+breakColName+" [removedColClazz="+removedColClazz+"]");
						}
					}
				}
			}
			log.debug("breakColIndexes = "+breakColIndexes);
			if(breakColIndexes.size()>0) {
				breakColsAddColumnHeaderBefore = breakColsAddColumnHeaderBeforeProperty;
				breakColsAddColumnHeaderAfter = breakColsAddColumnHeaderAfterProperty;
				// breakColsAddColumnHeaderBefore & breakColsAddColumnHeaderAfter can't both be true
			}
			else {
				breakColsAddColumnHeaderBefore = false;
				breakColsAddColumnHeaderAfter = false;
			}
		}
		//breakColIndexes.addAll(lsColNames);
		//breakColIndexes.retainAll(breakColNamesProperty);
		breakColValues.clear();
	}
	
	protected String getTableStyleClass() {
		return (tableName!=null && !tableName.equals(""))?DataDumpUtils.xmlEscapeText(tableName):"datadump";
	}
	
	@Override
	public void dumpHeader(Writer fos) throws IOException {
		tablePrepend(fos);
		//if(prepend!=null && (!innerTable || xpendInnerTable)) { out(prepend, fos); }
		StringBuilder sb = new StringBuilder();
		sb.append("<table class=\""+getTableStyleClass()+"\">");
		if(dumpStyleNumericAlignRight) {
			appendStyleNumericAlignRight(sb);
		}
		if(dumpCaptionElement){
			//XXX: set caption?
			sb.append(nl()+"\t<caption>" + (schemaName!=null?schemaName+".":"") + tableName + "</caption>");
		}
		if(dumpColElement) {
			sb.append(nl()+"\t<colgroup>");
			for(int i=0;i<finalColNames.size();i++) {
				sb.append(nl()+"\t\t<col colname=\""+finalColNames.get(i)+"\" type=\""+finalColTypes.get(i).getSimpleName()+"\"/>");
			}
			sb.append(nl()+"\t</colgroup>");
		}
		sb.append("\n");
		//XXX: add thead?
		
		addTableHeaderRows(sb);
		
		//XXX: add tbody?
		out(sb.toString(), fos);
	}
	
	protected void addTableHeaderRows(StringBuilder sb) {
		//System.out.println("[1:beforeguess] onRowsColCount="+onRowsColCount+" ; onColsColCount="+onColsColCount);
		boolean dumpedAsLeast1row = false;
		if(pivotInfo.isPivotResultSet()) {
			DataDumpUtils.guessPivotCols(finalColNames, colSepPattern, colValSepPattern); //guess cols/rows, since measures may be present or not...
			//System.out.println("[2:afterguess ] onRowsColCount="+onRowsColCount+" ; onColsColCount="+onColsColCount);
			for(int cc=0;cc<pivotInfo.onColsColCount;cc++) {
				StringBuilder sbrow = new StringBuilder();
				String colname = null;
				boolean measuresRow = false;
				for(int i=0;i<finalColNames.size();i++) {
					String[] parts = finalColNames.get(i).split(colSepPattern);
					
					if(parts.length>cc) {
						if(i<pivotInfo.onRowsColCount) {
							sbrow.append("<th class=\"blank\""+
									(i<pivotInfo.onRowsColCount?" dimoncol=\"true\"":"")+
									"/>");
							measuresRow = true;
							//colname = DataDumpUtils.xmlEscapeText(parts[cc]);
						}
						else {
							//split...
							String[] p2 = parts[cc].split(colValSepPattern);
							if(p2.length>1) {
								String thValue = p2[1];
								String nullAttrib = "";
								if(nullPlaceholder.equals(thValue)) {
									thValue = nullPlaceholderReplacer;
									nullAttrib = " null=\"true\"";
								}
								sbrow.append("<th"+nullAttrib+">"+thValue+"</th>");
								colname = DataDumpUtils.xmlEscapeText(p2[0]);
							}
							else {
								sbrow.append("<th measure=\"true\">"+parts[cc]+"</th>");
								measuresRow = true;
							}
						}
					}
					else if(cc+1==pivotInfo.onColsColCount) {
						if(i<pivotInfo.onRowsColCount) {
							sbrow.append("<th dimoncol=\"true\" measure=\"true\">"+finalColNames.get(i)+"</th>");
							measuresRow = true;
						}
						else {
							sbrow.append("<th>"+finalColNames.get(i)+"</th>");
						}
					}
					else {
						sbrow.append("<th class=\"blank\""+
								(i<pivotInfo.onRowsColCount?" dimoncol=\"true\"":"")+
								"/>");
					}
				}
				sb.append(nl()+"\t<tr"+(colname!=null?" colname=\""+colname+"\"":"")+(measuresRow?" measuresrow=\"true\"":"")+">");
				sb.append(sbrow);
				sb.append("</tr>");
				dumpedAsLeast1row = true;
			}
		}
		boolean dumpHeaderRow = !dumpedAsLeast1row &&
				(!innerTable || innerArrayDumpHeader || finalColNames.size()!=1) &&
				!breakColsAddColumnHeaderBefore && !breakColsAddColumnHeaderAfter;
		//log.info("dumpHeaderRow=="+dumpHeaderRow+" ;; dumpedAsLeast1row="+dumpedAsLeast1row+" ; innerTable="+innerTable+" ; innerArrayDumpHeader="+innerArrayDumpHeader+" ; finalColNames.size()="+finalColNames.size());
		if(dumpHeaderRow) {
			appendTableHeaderRow(sb);
		}
		//sb.append("\n");
	}
	
	protected void appendTableHeaderRow(StringBuilder sb) {
		sb.append(padd()+"\t<tr>");
		for(int i=0;i<finalColNames.size();i++) {
			sb.append("<th>"+finalColNames.get(i)+"</th>");
		}
		sb.append("</tr>\n");
	}
	
	/*
	protected void guessPivotCols() {
		//int prevCC = onColsColCount;
		int prevRC = onRowsColCount;
		
		onColsColCount = 0;
		onRowsColCount = 0;
		for(int i=0;i<finalColNames.size();i++) {
			int l = finalColNames.get(i).split(colSepPattern).length;
			if(l>1) {
				if(l>onColsColCount) {
					onColsColCount = l;
					onRowsColCount = i;
					break;
				}
			}
		}
		
		if(onColsColCount==0 && onRowsColCount==0) {
			for(int i=0;i<finalColNames.size();i++) {
				int l2 = finalColNames.get(i).split(colValSepPattern).length;
				if(l2>1) {
					onColsColCount = 1;
					onRowsColCount = i;
					break;
				}
			}
		}
		
		if(onColsColCount==0 && onRowsColCount==0) { //onRowsColCount+1==prevRC) {
			// when onColsColCount==0, "guess" is not effective
			onRowsColCount = prevRC;
		}
	}
	*/
	
	protected void appendStyleNumericAlignRight(StringBuilder sb) {
		List<String> styleSelector = new ArrayList<String>();
		for(int i=0;i<finalColNames.size();i++) {
			if(DataDumpUtils.isNumericType(finalColTypes.get(i))) {
				styleSelector.add("table."+getTableStyleClass()+" > tbody > tr > td:nth-child("+(i+1)+")");
			}
		}
		if(styleSelector.size()>0) {
			sb.append("\n\t<style>\n\t\t").append(Utils.join(styleSelector, ", ")).append(" { text-align: right; }\n\t</style>");
		}
	}

	@Override
	public void dumpRow(ResultSet rs, long count, Writer fos) throws IOException, SQLException {
		dumpRow(rs, count, null, fos);
	}
	
	public void dumpRow(ResultSet rs, long count, String clazz, Writer fos) throws IOException, SQLException {
		StringBuilder sb = new StringBuilder();
		List<Object> vals = SQLUtils.getRowObjectListFromRS(rs, lsColTypes, numCol, true);
		appendBreaksIfNeeded(vals, clazz, sb);
		sb.append("\t"+"<tr"+(clazz!=null?" class=\""+DataDumpUtils.xmlEscapeText(clazz)+"\"":"")+">");
		for(int i=0;i<finalColNames.size();i++) {
			Object origVal = vals.get(i);
			Class<?> ctype = finalColTypes.get(i);
			boolean isResultSet = DataDumpUtils.isResultSet(ctype, origVal);
			boolean isArray = DataDumpUtils.isArray(ctype, origVal);
			if(isResultSet || isArray) {
				ResultSet rsInt = null;
				if(isArray) {
					rsInt = DataDumpUtils.getResultSetFromArray(origVal, false, finalColNames.get(i));
				}
				else {
					rsInt = (ResultSet) origVal;
				}
				
				if(rsInt==null) {
					//log.warn("ResultSet is null");
					sb.append("<td></td>");
					continue;
				}
				
				out(sb.toString()+"<td>\n", fos);
				sb = new StringBuilder();
				
				HTMLDataDump htmldd = innerClone();
				//HTMLDataDump htmldd = new HTMLDataDump(this.padding+"\t\t", true);
				//htmldd.padding = this.padding+"\t\t";
				//log.info(":: "+rsInt+" / "+lsColNames);
				//htmldd.procProperties(prop);
				DataDumpUtils.dumpRS(htmldd, rsInt.getMetaData(), rsInt, null, finalColNames.get(i), fos, true);
				sb.append("\n\t</td>");
			}
			else {
				String value = DataDumpUtils.getFormattedXMLValue(origVal, ctype, floatFormatter, dateFormatter, nullValueStr,
						doEscape(i));
				//Object value = getValueNotNull( vals.get(i) );
				//sb.append( "<td" + (origVal==null?" null=\"true\"":"") + ">"+ value +"</td>");
				sb.append("<td"
						+(origVal==null?" null=\"true\"":"")
						+(i<getOnRowsColCount()?" dimoncol=\"true\"":"")
						+(dumpColType?" coltype=\""+ctype.getSimpleName()+"\"":"")
						+((dumpIsNumeric && DataDumpUtils.isNumericType(ctype))?" numeric=\"true\"":"")
						+">"+ value +"</td>");
			}
		}
		sb.append("</tr>");
		out(sb.toString()+"\n", fos);
	}
	
	protected void appendBreaksIfNeeded(List<Object> vals, String clazz, StringBuilder sb) {
		List<Object> breakRowValues = getIndexedValues(vals, breakColIndexes);
		if(breakRowValues!=null && !breakRowValues.equals(breakColValues)) {
			//log.debug("breaking table: breakRowValues = "+breakRowValues);
			breakColValues.clear();
			breakColValues.addAll(breakRowValues);
			if(breakColsAddColumnHeaderBefore) {
				appendTableHeaderRow(sb);
			}
			appendBreakRow(breakRowValues, clazz, sb);
			if(breakColsAddColumnHeaderAfter) {
				appendTableHeaderRow(sb);
			}
		}
	}
	
	protected void appendBreakRow(List<Object> breakRowValues, String clazz, StringBuilder sb) {
		sb.append("\t"+"<tr"+(clazz!=null?" class=\""+DataDumpUtils.xmlEscapeText(clazz)+"\"":"")+">");
		//sb.append("<td colspan=\""+finalColNames.size()+"\">"+breakRowValues+"</td>");
		//XXX: use 'th' or 'td'? 
		sb.append("<th colspan=\""+finalColNames.size()+"\">"+getBreakValuesRow(breakRowValues)+"</th>");
		sb.append("</tr>\n");
	}

	@Override
	public void dumpFooter(long count, boolean hasMoreRows, Writer fos) throws IOException {
		//XXX: add /tbody?
		out("</table>", fos);
		//if(append!=null && (!innerTable || xpendInnerTable)) { out(append, fos); }
		tableAppend(fos);
	}
	
	protected <T> List<T> getIndexedValues(List<T> vals, List<Integer> idxs) {
		if(idxs==null || idxs.size()==0) { return null; }
		List<T> ret = new ArrayList<T>();
		for(Integer i: idxs) {
			ret.add(vals.get(i));
		}
		return ret;
	}
	
	protected String getBreakValuesRow(List<Object> breakRowValues) {
		StringBuilder sb = new StringBuilder();
		List<String> breakColNames = getIndexedValues(lsColNames, breakColIndexes);
		for(int i=0;i<breakColIndexes.size();i++) {
			if(i>0) {
				sb.append(", ");
			}
			Object value = breakRowValues.get(i);
			if(value==null) { value = nullPlaceholderReplacer; } // UNICODE_NULL
			sb.append(breakColNames.get(i)+": "+value);
		}
		return sb.toString();
	}

	protected void tablePrepend(Writer fos) throws IOException {
		if(prepend!=null && (!innerTable || xpendInnerTable)) { out(prepend, fos); }
	}
	
	protected void tableAppend(Writer fos) throws IOException {
		if(append!=null && (!innerTable || xpendInnerTable)) { out(append, fos); }
	}
	
	/*protected void out(String s, Writer pw) throws IOException {
		pw.write(padding+s);
	}*/
	
	@Override
	public String getSyntaxId() {
		return HTML_SYNTAX_ID;
	}

	@Override
	public String getMimeType() {
		return "text/html";
	}
	
	@Override
	public void updateProperties(DumpSyntax ds) {
		if(! (ds instanceof HTMLDataDump)) {
			throw new RuntimeException(ds.getClass()+" must be instance of "+this.getClass());
		}
		HTMLDataDump dd = (HTMLDataDump) ds;
		super.updateProperties(dd);
		
		dd.append = this.append;
		dd.colSep = this.colSep;
		dd.colSepPattern = this.colSepPattern;
		dd.colValSep = this.colValSepPattern;
		dd.dumpCaptionElement = this.dumpCaptionElement;
		dd.dumpColElement = this.dumpColElement;
		dd.dumpStyleNumericAlignRight = this.dumpStyleNumericAlignRight;
		dd.innerTable = this.innerTable;
		dd.pivotInfo = this.pivotInfo;
		//dd.onColsColCount = this.onColsColCount;
		//dd.onRowsColCount = this.onRowsColCount;
		//dd.padding = this.padding;
		dd.prepend = this.prepend;
		dd.xpendInnerTable = this.xpendInnerTable;
	}
	
	@Override
	public HTMLDataDump clone() throws CloneNotSupportedException {
		HTMLDataDump dd = (HTMLDataDump) super.clone();
		//updateProperties(dd);
		dd.finalColNames = new ArrayList<String>();
		dd.finalColTypes = new ArrayList<Class<?>>();
		return dd;
	}
	
	@Override
	public HTMLDataDump innerClone() {
		try {
			HTMLDataDump dd = (HTMLDataDump) clone();
			dd.padding += "\t\t";
			dd.innerTable = true;
			return dd;
		}
		catch(CloneNotSupportedException e) {
			throw new RuntimeException(e);
		}
	}
	
	public int getOnRowsColCount() {
		return pivotInfo.onRowsColCount;
	}

}

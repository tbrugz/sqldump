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

	protected String padding;
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
	
	protected int onRowsColCount = 0;
	protected int onColsColCount = 0;
	protected String colSep = null;
	protected String colValSep = null;
	protected String colSepPattern = null;
	protected String colValSepPattern = null;
	protected static final String nullPlaceholder = PivotResultSet.NULL_PLACEHOLDER;
	protected static final String nullPlaceholderReplacer = UNICODE_NULL;
	
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
		procPivotProperties(prop);
	}

	public void procPivotProperties(Properties prop) {
		onRowsColCount = 0;
		onColsColCount = 0;
		String onrows = prop.getProperty(PROP_PIVOT_ONROWS);
		String oncols = prop.getProperty(PROP_PIVOT_ONCOLS);
		if(onrows!=null) {
			onRowsColCount = onrows.split(",").length;
		}
		if(oncols!=null) {
			onColsColCount = oncols.split(",").length;
		}
		if(isPivotResultSet()) {
			colSep = prop.getProperty(PROP_PIVOT_COLSEP, PivotResultSet.COLS_SEP);
			colValSep = prop.getProperty(PROP_PIVOT_COLVALSEP, PivotResultSet.COLVAL_SEP);
			colSepPattern = Pattern.quote(colSep);
			colValSepPattern = Pattern.quote(colValSep);
		}
	}
	
	public boolean isPivotResultSet() {
		return onRowsColCount>0 || onColsColCount>0;
	}
	
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
			sb.append("\n\t<caption>" + (schemaName!=null?schemaName+".":"") + tableName + "</caption>");
		}
		if(dumpColElement) {
			sb.append("\n<colgroup>");
			for(int i=0;i<finalColNames.size();i++) {
				sb.append("\n\t<col colname=\""+finalColNames.get(i)+"\" type=\""+finalColTypes.get(i).getSimpleName()+"\"/>");
			}
			sb.append("\n</colgroup>");
		}
		//XXX: add thead?
		
		addTableHeaderRows(sb);
		
		//XXX: add tbody?
		out(sb.toString(), fos);
	}
	
	protected void addTableHeaderRows(StringBuilder sb) {
		//System.out.println("[1:beforeguess] onRowsColCount="+onRowsColCount+" ; onColsColCount="+onColsColCount);
		boolean dumpedAsLeast1row = false;
		if(isPivotResultSet()) {
			guessPivotCols(); //guess cols/rows, since measures may be present or not...
			//System.out.println("[2:afterguess ] onRowsColCount="+onRowsColCount+" ; onColsColCount="+onColsColCount);
			for(int cc=0;cc<onColsColCount;cc++) {
				StringBuilder sbrow = new StringBuilder();
				String colname = null;
				boolean measuresRow = false;
				for(int i=0;i<finalColNames.size();i++) {
					String[] parts = finalColNames.get(i).split(colSepPattern);
					
					if(parts.length>cc) {
						if(i<onRowsColCount) {
							sbrow.append("<th class=\"blank\""+
									(i<onRowsColCount?" dimoncol=\"true\"":"")+
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
					else if(cc+1==onColsColCount) {
						if(i<onRowsColCount) {
							sbrow.append("<th dimoncol=\"true\" measure=\"true\">"+finalColNames.get(i)+"</th>");
							measuresRow = true;
						}
						else {
							sbrow.append("<th>"+finalColNames.get(i)+"</th>");
						}
					}
					else {
						sbrow.append("<th class=\"blank\""+
								(i<onRowsColCount?" dimoncol=\"true\"":"")+
								"/>");
					}
				}
				sb.append("\n\t<tr"+(colname!=null?" colname=\""+colname+"\"":"")+(measuresRow?" measuresrow=\"true\"":"")+">");
				sb.append(sbrow);
				sb.append("</tr>");
				dumpedAsLeast1row = true;
			}
		}
		boolean dumpHeaderRow = !dumpedAsLeast1row && (!innerTable || innerArrayDumpHeader || finalColNames.size()!=1);
		//log.info("dumpHeaderRow=="+dumpHeaderRow+" ;; dumpedAsLeast1row="+dumpedAsLeast1row+" ; innerTable="+innerTable+" ; innerArrayDumpHeader="+innerArrayDumpHeader+" ; finalColNames.size()="+finalColNames.size());
		if(dumpHeaderRow) {
			sb.append("\n\t<tr>");
			for(int i=0;i<finalColNames.size();i++) {
				sb.append("<th>"+finalColNames.get(i)+"</th>");
			}
			sb.append("</tr>");
		}
		sb.append("\n");
	}
	
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
		sb.append("\t"+"<tr"+(clazz!=null?" class=\""+DataDumpUtils.xmlEscapeText(clazz)+"\"":"")+">");
		List<Object> vals = SQLUtils.getRowObjectListFromRS(rs, lsColTypes, numCol, true);
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
						+(i<onRowsColCount?" dimoncol=\"true\"":"")
						+(dumpColType?" coltype=\""+ctype.getSimpleName()+"\"":"")
						+((dumpIsNumeric && DataDumpUtils.isNumericType(ctype))?" numeric=\"true\"":"")
						+">"+ value +"</td>");
				
			}
		}
		sb.append("</tr>");
		out(sb.toString()+"\n", fos);
	}

	@Override
	public void dumpFooter(long count, boolean hasMoreRows, Writer fos) throws IOException {
		//XXX: add /tbody?
		out("</table>", fos);
		//if(append!=null && (!innerTable || xpendInnerTable)) { out(append, fos); }
		tableAppend(fos);
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
		dd.onColsColCount = this.onColsColCount;
		dd.onRowsColCount = this.onRowsColCount;
		dd.padding = this.padding;
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

}

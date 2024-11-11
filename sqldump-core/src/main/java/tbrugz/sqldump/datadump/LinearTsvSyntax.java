package tbrugz.sqldump.datadump;

import java.io.IOException;
import java.io.Writer;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import tbrugz.sqldump.util.SQLUtils;
import tbrugz.sqldump.util.Utils;

/**
 * See: https://dataprotocols.org/linear-tsv/
 * and: TSVDataDump
 */
public class LinearTsvSyntax extends AbstractDumpSyntax implements Cloneable, DumpSyntaxBuilder {

	static final String LINEARTSV_SYNTAX_ID = "linear-tsv";
	static final String LINEARTSV_MIMETYPE = "text/tab-separated-values";

	static final char ROW_SEPARATOR = '\n';
	static final char COL_SEPARATOR = '\t';
	static final String NULL_VALUE = "\\N";

	static final boolean DEFAULT_COLUMNNAMESHEADER = true;

	boolean doColumnNamesHeaderDump = DEFAULT_COLUMNNAMESHEADER;

	//boolean useTextPlainMimeType = true;
	
	@Override
	public String getSyntaxId() { 
		return LINEARTSV_SYNTAX_ID;
	}
	
	@Override
	public String getDefaultFileExtension() {
		return "tsv"; // also '.tab'?
	}

	@Override
	public String getMimeType() {
		return LINEARTSV_MIMETYPE;
	}

	/*
	String fullPrefix() {
		return DEFAULT_DATADUMP_PREFIX + getSyntaxId() + ".";
	}
	*/
	
	public static String escape(String s) {
		StringBuilder sb = new StringBuilder();
		for(int i=0;i<s.length();i++) {
			char c = s.charAt(i);
			if(c=='\n') {
				sb.append("\\n");
			}
			else if(c=='\t') {
				sb.append("\\t");
			}
			else if(c=='\r') {
				sb.append("\\r");
			}
			else if(c=='\\') {
				sb.append("\\\\");
			}
			else {
				sb.append(c);
			}
		}
		return sb.toString();
	}
	
	public String getFormattedValue(Object elem, Class<?> type) {
		if(elem == null) {
			return NULL_VALUE;
		}
		else if(Double.class.isAssignableFrom(type)) {
			return floatFormatter.format(elem);
		}
		else if(Date.class.isAssignableFrom(type) && elem instanceof Date) {
			return dateFormatter.format((Date)elem);
		}
		else if(ResultSet.class.isAssignableFrom(type)) {
			return NULL_VALUE;
		}

		String val = DataDumpUtils.getPrintableString(elem);
		return escape(val);
	}
	
	@Override
	public void procProperties(Properties prop) {
		procStandardProperties(prop);
		doColumnNamesHeaderDump = Utils.getPropBool(prop, fullPrefix() + CSVDataDump.SUFFIX_COLUMNNAMESHEADER, DEFAULT_COLUMNNAMESHEADER);
		postProcProperties();
	}

	@Override
	public void dumpHeader(Writer w) throws IOException {
		if(doColumnNamesHeaderDump) {
			StringBuilder sb = new StringBuilder();
			for(int i=0;i<numCol;i++) {
				sb.append( (i!=0?COL_SEPARATOR:"") + escape(lsColNames.get(i)) );
			}
			sb.append(ROW_SEPARATOR);
			w.write(sb.toString());
		}
	}

	@Override
	public void dumpRow(ResultSet rs, long count, Writer w) throws IOException, SQLException {
		List<?> vals = SQLUtils.getRowObjectListFromRS(rs, lsColTypes, numCol);
		
		StringBuilder sb = new StringBuilder();
		//log.info("lsColTypes:: "+lsColTypes.size()+" / "+lsColTypes+" vals: "+vals.size()); 
		for(int i=0;i<lsColTypes.size();i++) {
			sb.append( (i!=0?COL_SEPARATOR:"") + getFormattedValue(vals.get(i), lsColTypes.get(i)) );
		}
		sb.append(ROW_SEPARATOR);
		w.write(sb.toString());
	}

	@Override
	public void dumpFooter(long count, boolean hasMoreRows, Writer fos) throws IOException {
	}

}

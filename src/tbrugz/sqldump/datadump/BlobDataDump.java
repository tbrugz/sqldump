package tbrugz.sqldump.datadump;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.sql.Blob;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.util.IOUtil;
import tbrugz.sqldump.util.SQLUtils;
import tbrugz.sqldump.util.Utils;

/*
 * add support for blobs (file: <tablename>_<pkcolumns>_<pkid>.blob ? specific prop !) - if table has no PK, no blob dumping (?)
 */
public class BlobDataDump extends DumpSyntax {

	static Log log = LogFactory.getLog(BlobDataDump.class);
	
	static final String BLOB_SYNTAX_ID = "blob";
	static final String PROP_BLOB_OUTFILEPATTERN = "sqldump.datadump.blob.outfilepattern";
	static final String PREFIX_BLOB_COLUMNS = "sqldump.datadump.blob.columns2dump@";
	
	static final String REGEX_COLUMNNAME = "\\[columnname\\]";
	static final String REGEX_ROWID = "\\[rowid\\]";

	// variables from properties
	Properties prop;
	String outFilePattern;
	
	// variables from query/table
	String tableName;
	List<String> pkCols;
	List<String> columnsToDump;
	
	List<String> lsColNames = new ArrayList<String>();
	List<Class<?>> lsColTypes = new ArrayList<Class<?>>();
	
	@Override
	public void procProperties(Properties prop) {
		outFilePattern = prop.getProperty(PROP_BLOB_OUTFILEPATTERN);
		if(outFilePattern==null) {
			log.warn("prop '"+PROP_BLOB_OUTFILEPATTERN+"' must be set");
			//XXX: if not found, use DataDump.PROP_DATADUMP_OUTFILEPATTERN
		}
		this.prop = prop;
	}

	@Override
	public String getSyntaxId() {
		return BLOB_SYNTAX_ID;
	}

	@Override
	public void initDump(String tableName, List<String> pkCols,
			ResultSetMetaData md) throws SQLException {
		this.tableName = tableName;
		this.pkCols = pkCols;
		
		int numCol = md.getColumnCount();		
		lsColNames.clear();
		lsColTypes.clear();
		if(pkCols==null) {
			//XXX: blob dump really needs PK/UK?
			log.warn("can't dump: needs unique key [query/table: "+tableName+"]");
			return;
		}
		
		for(int i=0;i<numCol;i++) {
			lsColNames.add(md.getColumnName(i+1));
		}
		for(int i=0;i<numCol;i++) {
			lsColTypes.add(SQLUtils.getClassFromSqlType(md.getColumnType(i+1), md.getPrecision(i+1), md.getScale(i+1)));
		}
		
		columnsToDump = Utils.getStringListFromProp(prop, PREFIX_BLOB_COLUMNS+tableName, ",");
	}

	@Override
	public void dumpHeader(Writer fos) {
	}
	
	//XX: prop for setting ROWID_JOINER
	static String ROWID_JOINER = "_";
	
	@Override
	public void dumpRow(ResultSet rs, long count, Writer writer) throws IOException, SQLException {
		if(pkCols==null) { return; }
		
		List<String> pkVals = new ArrayList<String>();
		for(String pkcol: pkCols) {
			pkVals.add(rs.getString(pkcol));
		}
		String rowid = Utils.join(pkVals, ROWID_JOINER);
		for(int i=0;i<lsColNames.size();i++) {
			String colName = lsColNames.get(i);
			Class<?> c = lsColTypes.get(i);
			if(columnsToDump!=null) {
				if(!columnsToDump.contains(colName)) { continue; } 
			}
			else if(! c.equals(Blob.class)) { continue; } //Clob also?
			
			String filename = outFilePattern
					.replaceAll(DataDump.PATTERN_TABLENAME_FINAL, Matcher.quoteReplacement(tableName) )
					//.replaceAll("${pkcolumnnames}", "")
					.replaceAll(REGEX_COLUMNNAME, Matcher.quoteReplacement(colName) ) //table may have more than 1 blob per row
					.replaceAll(REGEX_ROWID, Matcher.quoteReplacement(rowid) ) //pkid? rowid?
					.replaceAll(DataDump.PATTERN_SYNTAXFILEEXT_FINAL, BLOB_SYNTAX_ID);
			log.debug("blob filename: "+filename+"; col: "+colName);
			
			//open file, open blob, write content, close both
			try {
				InputStream is = rs.getBinaryStream(colName);
				File dir = new File(filename).getParentFile();
				dir.mkdirs();
				FileOutputStream fos = new FileOutputStream(filename);
				IOUtil.pipeStreams(is, fos);
				is.close();
				fos.close();
			}
			catch(SQLException e) {
				log.warn("sql error: "+e.getMessage()+"; errcode="+e.getErrorCode()+"; mixing BlobDataDump with other dumpers may cause problems");
				log.debug("sql error: "+e.getMessage(), e);
				pkCols = null; //warn only once per table/query
			}
		}
	}

	@Override
	public void dumpFooter(long count, Writer fos) {
	}
	
	@Override
	public boolean isWriterIndependent() {
		return true;
	}

	// http://en.wikipedia.org/wiki/Internet_media_type / http://stackoverflow.com/questions/6783921/which-mime-type-to-use-for-a-binary-file-thats-specific-to-my-program
	@Override
	public String getMimeType() {
		return "application/octet-stream";
	}

}

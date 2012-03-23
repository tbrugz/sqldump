package tbrugz.sqldump.datadump;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.Writer;
import java.sql.Blob;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.SQLUtils;
import tbrugz.sqldump.util.IOUtil;
import tbrugz.sqldump.util.Utils;

/*
 * add support for blobs (file: <tablename>_<pkcolumns>_<pkid>.blob ? specific prop !) - if table has no PK, no blob dumping (?)
 */
public class BlobDataDump extends DumpSyntax {

	static Log log = LogFactory.getLog(BlobDataDump.class);
	
	static final String BLOB_SYNTAX_ID = "blob";
	static final String PROP_BLOB_OUTFILEPATTERN = "sqldump.datadump.blob.outfilepattern";
	
	static final String REGEX_COLUMNNAME = "\\$\\{columnname\\}";
	static final String REGEX_ROWID = "\\$\\{rowid\\}";

	String tableName;
	List<String> pkCols;
	String outFilePattern;
	
	List<String> lsColNames = new ArrayList<String>();
	List<Class> lsColTypes = new ArrayList<Class>();
	
	@Override
	public void procProperties(Properties prop) {
		outFilePattern = prop.getProperty(PROP_BLOB_OUTFILEPATTERN);
		if(outFilePattern==null) {
			log.warn("prop '"+PROP_BLOB_OUTFILEPATTERN+"' must be set");
			//XXX: if not found, use DataDump.PROP_DATADUMP_OUTFILEPATTERN
		}
	}

	@Override
	public String getSyntaxId() {
		return BLOB_SYNTAX_ID;
	}

	@Override
	public void initDump(String tableName, List<String> pkCols,
			ResultSetMetaData md) throws Exception {
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
			lsColTypes.add(SQLUtils.getClassFromSqlType(md.getColumnType(i+1), md.getScale(i+1)));
		}
	}

	@Override
	public void dumpHeader(Writer fos) throws Exception {
	}
	
	//XX: prop for setting ROWID_JOINER
	public static String ROWID_JOINER = "_";
	
	@Override
	public void dumpRow(ResultSet rs, long count, Writer writer) throws Exception {
		if(pkCols==null) { return; }
		
		List<String> pkVals = new ArrayList<String>();
		for(String pkcol: pkCols) {
			pkVals.add(rs.getString(pkcol));
		}
		String rowid = Utils.join(pkVals, ROWID_JOINER);
		for(int i=0;i<lsColNames.size();i++) {
			Class c = lsColTypes.get(i);
			if(! c.equals(Blob.class)) { continue; }
			
			String filename = outFilePattern.replaceAll(DataDump.FILENAME_PATTERN_TABLENAME, tableName)
					//.replaceAll("${pkcolumnnames}", "")
					.replaceAll(REGEX_COLUMNNAME, lsColNames.get(i)) //table may have more than 1 blob per row
					.replaceAll(REGEX_ROWID, rowid) //pkid? rowid?
					.replaceAll(DataDump.FILENAME_PATTERN_SYNTAXFILEEXT, BLOB_SYNTAX_ID);
			log.debug("blob filename: "+filename+"; col: "+lsColNames.get(i));
			
			//open file, open blob, write content, close both
			try {
				InputStream is = rs.getBinaryStream(lsColNames.get(i));
				File dir = new File(filename).getParentFile();
				dir.mkdirs();
				FileOutputStream fos = new FileOutputStream(filename);
				IOUtil.pipeStreams(is, fos);
				is.close();
				fos.close();
			}
			catch(SQLException e) {
				log.warn("sql error: "+e.getMessage()+"; errcode="+e.getErrorCode()+"; mixin BlobDataDump with other dumpers may cause problems");
				pkCols = null; //warn only once per table/query
			}
		}
	}

	@Override
	public void dumpFooter(Writer fos) throws Exception {
	}
	
	@Override
	public boolean isWriterIndependent() {
		return true;
	}

}

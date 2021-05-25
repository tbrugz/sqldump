package tbrugz.sqldump.sqlrun.importers;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.datadump.DataDumpUtils;
import tbrugz.sqldump.def.AbstractFailable;
import tbrugz.sqldump.sqlrun.def.CommitStrategy;
import tbrugz.sqldump.sqlrun.def.Constants;
import tbrugz.sqldump.sqlrun.def.Importer;
import tbrugz.sqldump.util.Utils;

public abstract class BaseImporter extends AbstractFailable implements Importer {

	static final Log log = LogFactory.getLog(BaseImporter.class);
	
	String execId;
	Properties prop;
	Connection conn;
	CommitStrategy commitStrategy;
	String defaultInputEncoding = DataDumpUtils.CHARSET_UTF8;
	
	//boolean useBatchUpdate = false;
	//long batchUpdateSize = 1000;
	String insertTable = null;
	String insertSQL = null;
	List<String> columnNames;
	List<String> columnTypes;
	List<Integer> filecol2tabcolMap = null;
	boolean doCreateTable = false;

	//XXX: different exec suffixes for each importer class?
	static final String[] EXEC_SUFFIXES = {
		Constants.SUFFIX_IMPORT,
	};
	
	@Override
	public String getExecId() {
		return execId;
	}

	@Override
	public void setExecId(String execId) {
		this.execId = execId;
	}

	@Override
	public void setProperties(Properties prop) {
		this.prop = prop;
		
		String importerPrefix = getImporterPrefix();
		insertTable = prop.getProperty(importerPrefix + Constants.SUFFIX_INSERTTABLE);
		insertSQL = prop.getProperty(importerPrefix + Constants.SUFFIX_INSERTSQL);
		if(insertTable!=null && insertSQL!=null) {
			log.warn("both "+Constants.SUFFIX_INSERTTABLE+" & "+Constants.SUFFIX_INSERTSQL+" defined. Will reset "+Constants.SUFFIX_INSERTTABLE);
			insertTable = null;
		}
		//log.info("importerPrefix = "+importerPrefix+"; insertTable =  "+insertTable);
		columnTypes = Utils.getStringListFromProp(prop, importerPrefix + Constants.SUFFIX_COLUMN_TYPES, ",");
		doCreateTable = Utils.getPropBool(prop, importerPrefix + Constants.SUFFIX_DO_CREATE_TABLE, false);
	}
	
	@Override
	public void setConnection(Connection conn) {
		this.conn = conn;
	}

	@Override
	public void setCommitStrategy(CommitStrategy commitStrategy) {
		this.commitStrategy = commitStrategy;
	}

	@Override
	public void setDefaultFileEncoding(String encoding) {
		this.defaultInputEncoding = encoding;
	}

	@Override
	public List<String> getExecSuffixes() {
		List<String> ret = new ArrayList<String>();
		ret.addAll(Arrays.asList(EXEC_SUFFIXES));
		return ret;
	}
	
	public String getImporterPrefix() {
		return Constants.PREFIX_EXEC+execId;
	}
	
	PreparedStatement getStatement() throws SQLException {
		return conn.prepareStatement(getInsertSql());
	}
	
	void createTable() throws SQLException {
		String sql = getCreateTableSql();
		boolean b = conn.createStatement().execute(sql);
		log.info("create table, return = "+b);
		//log.info("create table, sql = ["+sql+"] return = "+b);
	}
	
	String getInsertSql() throws SQLException {
		String sql = null;
		if(insertSQL!=null) {
			sql = getInsertSql(insertSQL);
		}
		else if(columnNames==null) {
			sql = getInsertSql(insertTable, columnTypes.size());
		}
		else {
			sql = getInsertSql(insertTable, columnTypes.size(), columnNames);
		}
		return sql;
	}
	
	String getCreateTableSql() {
		if(columnNames==null) {
			columnNames = new ArrayList<String>();
		}
		return getCreateTableSql(insertTable, columnNames, columnTypes);
	}
	
	String getInsertSql(String insertSQL) throws SQLException {
		StringBuilder sb = new StringBuilder();
		
		log.debug("original insert sql: "+insertSQL);
		filecol2tabcolMap = new ArrayList<Integer>();
		int fromIndex = 0;
		while(true) {
			int ind1 = insertSQL.indexOf("${", fromIndex);
			//int indQuestion = insertSQL.indexOf("?", fromIndex);
			if(ind1<0) { break; }
			int ind2 = insertSQL.indexOf("}", ind1);
			//log.debug("ind/2: "+ind+" / "+ind2);
			int number = Integer.parseInt(insertSQL.substring(ind1+2, ind2));
			fromIndex = ind2;
			filecol2tabcolMap.add(number);
		}
		//XXX: mix ? and ${number} ?
		//filecol2tabcolMap = new int[parts.length];
		//for(int i=0;i<intl.size();i++) { filecol2tabcolMap[i] = intl.get(0); }
		String thisInsertSQL = insertSQL.replaceAll("\\$\\{[0-9]+\\}", "?");
		if(filecol2tabcolMap.size()>0) {
			log.debug("mapper: "+filecol2tabcolMap);
		}
		sb.append(thisInsertSQL);
		return sb.toString();
	}

	static String getInsertSql(String insertTable, int colCount) throws SQLException {
		StringBuilder sb = new StringBuilder();
		
		sb.append("insert into "+insertTable+ " values (");
		for(int i=0;i<colCount;i++) {
			sb.append((i==0?"":", ")+"?");
		}
		sb.append(")");
		return sb.toString();
	}
	
	static String getInsertSql(String insertTable, List<String> columnNames) throws SQLException {
		return getInsertSql(insertTable, columnNames.size(), columnNames);
	}
	
	static String getInsertSql(String insertTable, int colCount, List<String> columnNames) throws SQLException {
		StringBuilder sb = new StringBuilder();
		
		sb.append("insert into " + insertTable + " (");
		for(int i=0;i<colCount;i++) {
			sb.append((i==0?"":", ")+columnNames.get(i));
		}
		sb.append(") values (");
		for(int i=0;i<colCount;i++) {
			sb.append((i==0?"":", ")+"?");
		}
		sb.append(")");
		return sb.toString();
	}

	/* private */
	static String getCreateTableSql(String tableName, List<String> columnNames, List<String> columnTypes) {
		if(columnNames==null) {
			throw new IllegalArgumentException("columnNames must not be null");
		}
		if(columnTypes==null) {
			throw new IllegalArgumentException("columnTypes must not be null");
		}
		if(columnTypes.size()==0) {
			throw new IllegalStateException("columnTypes.size()=="+columnTypes.size());
		}

		StringBuilder sb = new StringBuilder();
		//log.info("colnames="+columnNames+"; coltypes="+columnTypes);
		
		sb.append("create table " + tableName + " (");
		if(columnNames.size()==0) {
			log.info("#colnames = "+columnNames.size());
			//columnNames.clear();
			for(int i=0;i<columnTypes.size();i++) {
				columnNames.add("C"+i);
			}
			//log.info("columnTypes: "+columnTypes);
		}
		if(columnNames.size()!=columnTypes.size()) {
			String message = "#columnNames ["+columnNames.size()+"] != #columnTypes ["+columnTypes.size()+"]";
			log.warn(message);
			log.info("columnNames: "+columnNames+" ; columnTypes: "+columnTypes);
			throw new IllegalStateException(message);
		}
		for(int i=0;i<columnTypes.size();i++) {
			sb.append((i==0?"":", ") + columnNames.get(i) + " " + getSqlColumnType(columnTypes.get(i)));
		}
		sb.append(")");
		return sb.toString();
	}

	int setStmtMappedValue(PreparedStatement stmt, String colType, int index, Object objValue) throws SQLException, ParseException {
		if(filecol2tabcolMap!=null && filecol2tabcolMap.size()>0) {
			int valsSetted = 0;
			for(int i=0;i<filecol2tabcolMap.size();i++) {
				int listIdx = filecol2tabcolMap.get(i);
				if(listIdx == index) {
					int colIndex = i;
					//log.debug("...setStmtMappedValue: "+index+"/"+colIndex+" ; objValue="+objValue+" ; colType="+colType );
					setStmtValue(stmt, colType, colIndex, objValue);
					valsSetted++;
				}
			}
			return valsSetted;
		}
		else {
			setStmtValue(stmt, colType, index, objValue);
			return 1;
		}
	}
	
	static void setStmtValue(PreparedStatement stmt, String colType, int index, Object objValue) throws SQLException, ParseException {
		if(objValue==null) {
			/* if(colType.equals("int") || colType.equals("double") || colType.equals("doublec")) { } */
			stmt.setObject(index+1, null);
			return;
		}
		String value = String.valueOf(objValue);
		if(colType!=null) {
			if(colType.equals("int")) {
				try {
					stmt.setInt(index+1, Integer.parseInt(value.trim()));
				}
				catch(NumberFormatException e) {
					stmt.setString(index+1, null);
					/*if(onErrorIntValue!=null) {
						stmt.setInt(index+1, onErrorIntValue);
					}
					else {
						throw e;
					}*/
				}
			}
			else if(colType.equals("double")) {
				stmt.setDouble(index+1, Double.parseDouble(value.replaceAll(",", "").trim()));
			}
			else if(colType.equals("doublec")) {
				stmt.setDouble(index+1, Double.parseDouble(value.replaceAll("\\.", "").replaceAll(",", ".").trim()));
			}
			else if(colType.equals("string")) {
				stmt.setString(index+1, value);
			}
			else if(colType.startsWith("date[")) {
				//XXX use java.sql.Timestamp?
				//XXX setup DateFormat only once for each column?
				String strFormat = colType.substring(5, colType.indexOf(']'));
				DateFormat df = new SimpleDateFormat(strFormat);
				stmt.setDate(index+1, new java.sql.Date( df.parse(value).getTime() ));
			}
			else if(colType.equals("blob-location") || colType.equals("text-location")) {
				File f = new File(value);
				if(!f.exists()) {
					log.warn("file '"+f+"' not found [col# = "+(index+1)+"]");
				}
				try {
					if(colType.equals("blob-location")) {
						//stmt.setBinaryStream(index+1, new FileInputStream(f));
						stmt.setBlob(index+1, new FileInputStream(f));
					}
					else if(colType.equals("text-location")) {
						//[inputstream]: asciistream ; [reader]: characterstream, clob, ncharacterstream, nclob
						stmt.setCharacterStream(index+1, new FileReader(f));
					}
					throw new IllegalStateException("unknown colType: "+colType);
				} catch (Exception e) {
					log.warn("Error importing '"+colType+"' file '"+f+"': "+e);
				}
			}
			else {
				String message = "setStmtValue: unknown columnType '"+colType+"' [#"+index+"]";
				log.warn(message);
				throw new IllegalArgumentException(message);
			}
			//XXX: more column types (boolean, byte, long, object?, null?, ...)
			return;
		}
		//default: set as string
		//log.debug("setStmtValue: index [="+(index+1)+"]: "+value);
		stmt.setString(index+1, value);
	}
	
	static String getSqlColumnType(String type) {
		if(type.startsWith("double")) { return "double precision"; }
		if(type.equals("int")) { return "integer"; }
		return "varchar";
	}

}

package tbrugz.sqldump;

import java.util.*;
import java.sql.*;
import java.io.*;
import java.math.BigDecimal;

import org.apache.log4j.Logger;

import tbrugz.sqldump.dbmodel.Column;
import tbrugz.sqldump.dbmodel.FK;
import tbrugz.sqldump.dbmodel.Grant;
import tbrugz.sqldump.dbmodel.Index;
import tbrugz.sqldump.dbmodel.PrivilegeType;
import tbrugz.sqldump.dbmodel.Table;
import tbrugz.sqldump.dbmodel.TableType;
import tbrugz.sqldump.graph.Schema2GraphML;

/*
 * XXXxxx (database dependent): DDL: grab contents from procedures, triggers and views 
 * TODO: option of data dump with INSERT INTO
 * XXX: detach main (SQLDataDump) from data dump
 * TODOne: generate graphml from schema structure
 * TODOne: column type mapping
 * TODOne: FK constraints at end of schema dump script?
 * TODOne: unique constraints? indexes? 
 * TODOne: sequences?
 * XXXdone: include Grants into SchemaModel?
 * TODO: recursive dump based on FKs
 * TODO: accept list of schemas, tables/objects to grab/dump, types of objects to grab/dump
 * XXX(later): usePrecision should be defined by java code (not .properties)
 * XXX(later): generate "alter table" database script from graphML changes (XMLUnit?)
 * XXXdone: dump dbobjects ordered by type (tables, fks, views, triggers, etc(functions, procedures, packages)), name
 * XXXdone: dump different objects to different files (using log4j - different loggers? no!)
 * XXXdone: more flexible output options (option to group or not grants|fks|index with tables - "group" means same file)
 * XXXdone: script output: option to group specific objects (fk, grants, index) with referencing table
 * XXXdone: script output: option to output specific objects (eg FK or Grants) with specific pattern 
 * XXXdone: compact grant syntax
 * TODO: postgresql specific features
 * XXX: derby/ansi specific features?
 * TODOne: bitbucket project's wiki
 * TODOne: main(): args: point to different .properties init files. 
 * XXXdone: Use ${xxx} params inside Properties
 * XXX: data dump: limit number of rows, tables to dump. define output patterns for data dump
 */
public class SQLDump {
	
	//connection properties
	static final String PROP_DRIVERCLASS = "sqldump.driverclass";
	static final String PROP_URL = "sqldump.dburl";
	static final String PROP_USER = "sqldump.user";
	static final String PROP_PASSWD = "sqldump.password";

	//sqldump.properties
	static final String PROP_DO_SCHEMADUMP = "sqldump.doschemadump";
	static final String PROP_DO_SCHEMADUMP_PKS = "sqldump.doschemadump.pks";
	static final String PROP_DO_SCHEMADUMP_FKS = "sqldump.doschemadump.fks";
	static final String PROP_DO_SCHEMADUMP_FKS_ATEND = "sqldump.doschemadump.fks.atend";
	static final String PROP_DO_SCHEMADUMP_GRANTS = "sqldump.doschemadump.grants";
	static final String PROP_DO_SCHEMADUMP_INDEXES = "sqldump.doschemadump.indexes";
	static final String PROP_FROM_DB_ID = "sqldump.fromdbid";
	static final String PROP_TO_DB_ID = "sqldump.todbid";
	static final String PROP_DUMP_WITH_SCHEMA_NAME = "sqldump.dumpwithschemaname";
	static final String PROP_DUMP_SYNONYM_AS_TABLE = "sqldump.dumpsynonymastable";
	static final String PROP_DUMP_VIEW_AS_TABLE = "sqldump.dumpviewastable";
	static final String PROP_DUMP_DBSPECIFIC = "sqldump.usedbspecificfeatures";
	
	static final String PROP_DO_TESTS = "sqldump.dotests";
	static final String PROP_DO_DATADUMP = "sqldump.dodatadump";
	static final String PROP_DUMPSCHEMAPATTERN = "sqldump.dumpschemapattern";
	
	static final String PROP_OUTPUTFILE = "sqldump.outputfile";
	
	//properties files filenames
	static final String PROPERTIES_FILENAME = "sqldump.properties";
	static final String COLUMN_TYPE_MAPPING_RESOURCE = "column-type-mapping.properties";
	
	static Logger log = Logger.getLogger(SQLDump.class);
	
	Connection conn;
	
	//tables OK for data dump
	List<String> tableNamesForDataDump = new Vector<String>();

	Properties papp = new ParametrizedProperties();
	Properties columnTypeMapping = new ParametrizedProperties();
	
	boolean doTests = false, doSchemaDump = false, doDataDump = false;
	//XXX: remove below?
	boolean doSchemaGrabPKs = false, doSchemaGrabFKs = false, doSchemaGrabGrants = false, doSchemaGrabIndexes = false;
	boolean doSchemaGrabDbSpecific = false;
	
	static final String PARAM_PROPERTIES_FILENAME = "-propfile="; 
	
	void init(String[] args) throws Exception {
		log.info("init...");
		String propFilename = PROPERTIES_FILENAME;
		for(String arg: args) {
			if(arg.indexOf(PARAM_PROPERTIES_FILENAME)==0) {
				propFilename = arg.substring(PARAM_PROPERTIES_FILENAME.length());
			}
			else {
				log.warn("unrecognized param '"+arg+"'. ignoring...");
			}
		}
		
		log.info("loading properties: "+propFilename);
		papp.load(new FileInputStream(propFilename));
		/*try {
			papp.load(new FileInputStream(propFilename));
		}
		catch(FileNotFoundException e) {
			log.warn("file "+propFilename+" not found. loading "+PROPERTIES_FILENAME);			
			papp.load(new FileInputStream(PROPERTIES_FILENAME));
		}*/
		
		//inicializa banco
		Class.forName(papp.getProperty(PROP_DRIVERCLASS));

		Properties p = new Properties();
		p.setProperty("user", papp.getProperty(PROP_USER, ""));
		p.setProperty("password", papp.getProperty(PROP_PASSWD, ""));

		conn = DriverManager.getConnection(papp.getProperty(PROP_URL), p);
		
		//inicializa variaveis controle
		doSchemaDump = papp.getProperty(PROP_DO_SCHEMADUMP, "").equals("true");
		doSchemaGrabPKs = papp.getProperty(PROP_DO_SCHEMADUMP_PKS, "").equals("true");
		doSchemaGrabFKs = papp.getProperty(PROP_DO_SCHEMADUMP_FKS, "").equals("true");
		//doSchemaDumpFKsAtEnd = papp.getProperty(PROP_DO_SCHEMADUMP_FKS_ATEND, "").equals("true");
		doSchemaGrabGrants = papp.getProperty(PROP_DO_SCHEMADUMP_GRANTS, "").equals("true");
		//dumpWithSchemaName = papp.getProperty(PROP_DUMP_WITH_SCHEMA_NAME, "").equals("true");
		//dumpSynonymAsTable = papp.getProperty(PROP_DUMP_SYNONYM_AS_TABLE, "").equals("true");
		//dumpViewAsTable = papp.getProperty(PROP_DUMP_VIEW_AS_TABLE, "").equals("true");
		doSchemaGrabIndexes = papp.getProperty(PROP_DO_SCHEMADUMP_INDEXES, "").equals("true");
		doSchemaGrabDbSpecific = papp.getProperty(PROP_DUMP_DBSPECIFIC, "").equals("true");

		columnTypeMapping.load(SQLDump.class.getClassLoader().getResourceAsStream(COLUMN_TYPE_MAPPING_RESOURCE));
		
		doTests = papp.getProperty(PROP_DO_TESTS, "").equals("true");
		doDataDump = papp.getProperty(PROP_DO_DATADUMP, "").equals("true"); 
	}

	void end() throws Exception {
		log.info("...done");
		conn.close();
	}

	SchemaModel grabSchema() throws Exception {
		log.info("schema dump...");
		DatabaseMetaData dbmd = conn.getMetaData();
		
		String schemaPattern = papp.getProperty(PROP_DUMPSCHEMAPATTERN,null);
		log.debug("schema pattern: "+schemaPattern);
		
		ResultSet rs = dbmd.getTables(null, schemaPattern, null, null);
		SchemaModel schemaModel = new SchemaModel();
		
		while(rs.next()) {
			TableType ttype = null;
			String tableName = rs.getString("TABLE_NAME");
			String schemaName = rs.getString("TABLE_SCHEM");
			
			tableNamesForDataDump.add(tableName);
			ttype = TableType.getTableType(rs.getString("TABLE_TYPE"), tableName);
			
			//defining model
			Table table = new Table();
			table.name = tableName;
			table.type = ttype;
			table.schemaName = schemaName;
			
			try {
				String fullTablename = (schemaPattern==null?"":schemaPattern+".")+tableName;
				log.debug("getting columns from "+fullTablename);

				//columns
				ResultSet cols = dbmd.getColumns(null, schemaPattern, tableName, null);
				while(cols.next()) {
					Column c = retrieveColumn(cols);
					table.columns.add(c);
					//String colDesc = getColumnDesc(c, columnTypeMapping, papp.getProperty(PROP_FROM_DB_ID), papp.getProperty(PROP_TO_DB_ID));
				}
				cols.close();
				
				//PKs
				if(doSchemaGrabPKs) {
					log.debug("getting PKs from "+fullTablename);
					ResultSet pks = dbmd.getPrimaryKeys(null, schemaPattern, tableName);
					grabSchemaPKs(pks, table);
					pks.close();
				}

				//FKs
				if(doSchemaGrabFKs) {
					log.debug("getting FKs from "+fullTablename);
					ResultSet fkrs = dbmd.getImportedKeys(null, schemaPattern, tableName);
					grabSchemaFKs(fkrs, table, schemaModel.foreignKeys);
					fkrs.close();
				}
				
				//GRANTs
				if(doSchemaGrabGrants) {
					log.debug("getting grants from "+fullTablename);
					ResultSet grantrs = dbmd.getTablePrivileges(null, schemaPattern, tableName);
					table.grants = grabSchemaGrants(grantrs, tableName);
					grantrs.close();
				}
				
				//INDEXes
				if(doSchemaGrabIndexes && TableType.TABLE.equals(table.type)) {
					log.debug("getting indexes from "+fullTablename);
					ResultSet indexesrs = dbmd.getIndexInfo(null, schemaPattern, tableName, false, false);
					grabSchemaIndexes(indexesrs, schemaModel.indexes);
					indexesrs.close();
				}
			}
			catch(OutOfMemoryError oome) {
				log.warn("OutOfMemoryError: memory: max: "+Runtime.getRuntime().maxMemory()+"; total: "+Runtime.getRuntime().totalMemory()+"; free: "+Runtime.getRuntime().freeMemory());
				throw oome;
			}
			catch(SQLException sqle) {
				log.warn("exception in table: "+tableName+" ["+sqle+"]");
				//sqle.printStackTrace();
				tableNamesForDataDump.remove(tableName);
			}
			
			schemaModel.tables.add(table);
		}
		rs.close();
		log.info(schemaModel.tables.size()+" tables grabbed");
		log.info(schemaModel.foreignKeys.size()+" FKs grabbed");
		if(doSchemaGrabIndexes) {
			log.info(schemaModel.indexes.size()+" indexes grabbed");
		}
		//log.debug("tables::["+schemaModel.tables.size()+"]\n"+schemaModel.tables+"\n");
		//log.debug("FKs::["+schemaModel.foreignKeys.size()+"]\n"+schemaModel.foreignKeys+"\n");

		if(doSchemaGrabDbSpecific) {
			grabDbSpecific(schemaModel, schemaPattern);
		}
		
		return schemaModel;
	}
	
	void grabDbSpecific(SchemaModel model, String schemaPattern) throws SQLException {
		//TODOne: test sqldump.usedbspeficicfeatures // set specific class in sqldump.properties?
		String dbSpecificFeaturesClass = columnTypeMapping.getProperty("dbms."+papp.getProperty(PROP_FROM_DB_ID)+".specificgrabclass");
		if(dbSpecificFeaturesClass!=null) {
			try {
				Class<?> c = Class.forName(dbSpecificFeaturesClass);
				DBMSFeatures of = (DBMSFeatures) c.newInstance();
				of.procProperties(papp);
				of.grabDBObjects(model, schemaPattern, conn);
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} catch (InstantiationException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			}
		}
	}
	
	static Column retrieveColumn(ResultSet cols) throws SQLException {
		Column c = new Column();
		c.name = cols.getString("COLUMN_NAME");
		c.type = cols.getString("TYPE_NAME");
		c.nullable = "YES".equals(cols.getString("IS_NULLABLE"));
		c.columSize = cols.getInt("COLUMN_SIZE");
		Object decimalDigits = cols.getObject("DECIMAL_DIGITS");
		if(decimalDigits!=null) {
			int iDecimalDigits = ((BigDecimal) decimalDigits).intValue();
			if(iDecimalDigits!=0) {
				c.decimalDigits = iDecimalDigits;
			} 
		}
		return c;
	}

	List<Grant> grabSchemaGrants(ResultSet grantrs, String tableName) throws SQLException {
		List<Grant> grantsList = new ArrayList<Grant>();
		while(grantrs.next()) {
			try {
				Grant grant = new Grant();
				
				grant.grantee = grantrs.getString("GRANTEE");
				grant.privilege = PrivilegeType.valueOf(Utils.normalizeEnumStringConstant(grantrs.getString("PRIVILEGE")));
				grant.table = grantrs.getString("TABLE_NAME");
				grant.withGrantOption = "YES".equals(grantrs.getString("IS_GRANTABLE"));
				grantsList.add(grant);
			}
			catch(IllegalArgumentException iae) {
				log.warn(iae);
			}
		}
		return grantsList;
	}
	
	void grabSchemaPKs(ResultSet pks, Table table) throws SQLException {
		//PKs
		Map<String, Set<String>> tablePK = new HashMap<String, Set<String>>();
		//ResultSet pks = dbmd.getPrimaryKeys(null, schemaPattern, tableName);
		int count=0;
		while(pks.next()) {
			String pkName = pks.getString("PK_NAME");
			if(pkName==null) {
				pkName = "PK_"+count;
				count++;
			}
			table.pkConstraintName = pkName;
			Set<String> pk = tablePK.get(pkName);
			if(pk==null) {
				pk = new HashSet<String>(); //XXXxx: TreeSet? no need...
				tablePK.put(pkName, pk);
			}
			pk.add(pks.getString("COLUMN_NAME"));
		}
		for(String key: tablePK.keySet()) {
			for(String colName: tablePK.get(key)) {
				table.getColumn(colName).pk = true;
			}
		}
		
	}

	void grabSchemaFKs(ResultSet fkrs, Table table, Set<FK> foreignKeys) throws SQLException, IOException {
		Map<String, FK> fks = new HashMap<String, FK>();
		int count=0;
		while(fkrs.next()) {
			//log.debug("FK!!!");
			String fkName = fkrs.getString("FK_NAME");
			if(fkName==null) {
				log.warn("nameless FK: "+fkrs.getString("FKTABLE_NAME")+"->"+fkrs.getString("PKTABLE_NAME"));
				fkName = "FK_"+count;
				count++;
			}
			log.debug("fk: "+fkName);
			FK fk = fks.get(fkName);
			if(fk==null) {
				fk = new FK();
				fk.setName(fkName);
				fks.put(fkName, fk);
			}
			if(fk.pkTable==null) {
				fk.pkTable = fkrs.getString("PKTABLE_NAME");
				fk.fkTable = fkrs.getString("FKTABLE_NAME");
				fk.pkTableSchemaName = fkrs.getString("PKTABLE_SCHEM");
				fk.fkTableSchemaName = fkrs.getString("FKTABLE_SCHEM");
			}
			fk.fkColumns.add(fkrs.getString("FKCOLUMN_NAME"));
			fk.pkColumns.add(fkrs.getString("PKCOLUMN_NAME"));
		}
		for(String key: fks.keySet()) {
			foreignKeys.add(fks.get(key));
		}
	}
	
	void grabSchemaIndexes(ResultSet indexesrs, Set<Index> indexes) throws SQLException {
		Index idx = null;
		
		while(indexesrs.next()) {
			String idxName = indexesrs.getString("INDEX_NAME");
			log.debug("index: "+idxName);
			if(idxName==null) {
				 //each table appears to have a no-name index, maybe "oracle-only"...
				log.debug("nameless index: "+indexesrs.getString("TABLE_NAME"));
				continue; 
			}
			if(idx==null || !idxName.equals(idx.name)) {
				//end last object
				if(idx!=null) {
					indexes.add(idx);
				}
				//new object
				idx = new Index();
				idx.name = idxName;
				idx.unique = indexesrs.getInt("NON_UNIQUE")==0;
				idx.schemaName = indexesrs.getString("TABLE_SCHEM");
				idx.tableName = indexesrs.getString("TABLE_NAME");
			}
			idx.columns.add(indexesrs.getString("COLUMN_NAME"));
		}
		if(idx!=null) {
			indexes.add(idx);
		}
	}
	
	void dumpData() throws Exception {
		FileWriter fos = new FileWriter(papp.getProperty(PROP_OUTPUTFILE));
		log.info("data dumping...");
		
		for(String table: tableNamesForDataDump) {
			Statement st = conn.createStatement();
			log.debug("dumping data from table: "+table);
			ResultSet rs = st.executeQuery("select * from \""+table+"\"");
			out("\n[table "+table+"]\n", fos);
			ResultSetMetaData md = rs.getMetaData();
			int numCol = md.getColumnCount();
			while(rs.next()) {
				out(getRowFromRS(rs, numCol, table), fos);
			}
			rs.close();
		}
		
		fos.close();
	}
	
	void out(String s, FileWriter fos) throws IOException {
		fos.write(s+"\n");
	}
	
	void tests() throws Exception {
		log.info("some tests...");

		DatabaseMetaData dbmd = conn.getMetaData();

		//log.info("test: catalogs...");
		//dumpRS(dbmd.getCatalogs());

		//log.info("test: table types...");
		//dumpRS(dbmd.getTableTypes());

		//log.info("test: tables...");
		//dumpRS(dbmd.getTables(null, null, null, null));

		//log.info("test: columns...");
		//dumpRS(dbmd.getColumns(null, "schema", "table", null));
		
		//log.info("test: fks...");
		//dumpRS(dbmd.getImportedKeys(null, "schema", "table"));

		//log.info("test: grants...");
		//dumpRS(dbmd.getTablePrivileges(null, "schema", "table"));
		
		//log.info("test: indexes...");
		//dumpRS(dbmd.getIndexInfo(null, "schema", "table", false, false));
		
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		SQLDump sdd = new SQLDump();

		sdd.init(args);
		
		if(sdd.doTests) {
			sdd.tests();
		}
		if(sdd.doSchemaDump) {
			SchemaModel sm = sdd.grabSchema();
			
			//script dump
			SchemaModelDumper schemaDumper = new SchemaModelScriptDumper();
			schemaDumper.procProperties(sdd.papp);
			schemaDumper.dumpSchema(sm);
			
			//graphml dump
			SchemaModelDumper s2gml = new Schema2GraphML();
			s2gml.procProperties(sdd.papp);
			s2gml.dumpSchema(sm);
		}
		if(sdd.doDataDump) {
			sdd.dumpData();
		}
		
		sdd.end();
	}
	
	

	//--------------- UTILS
	
	static void dumpRS(ResultSet rs) throws SQLException {
		dumpRS(rs, rs.getMetaData());
	}

	static void dumpRS(ResultSet rs, ResultSetMetaData rsmd) throws SQLException {
		int ncol = rsmd.getColumnCount();
		StringBuffer sb = new StringBuffer();
		//System.out.println(ncol);
		//System.out.println();
		for(int i=1;i<=ncol;i++) {
			//System.out.println(rsmd.getColumnName(i)+" | ");
			sb.append(rsmd.getColumnLabel(i)+" | ");
		}
		sb.append("\n");
		while(rs.next()) {
			for(int i=1; i<= rsmd.getColumnCount(); i++) {
				sb.append(rs.getString(i) + " | ");
			}
			sb.append("\n");
		}
		System.out.println("\n"+sb.toString()+"\n");
	}
	
	static StringBuffer sbTmp = new StringBuffer();
	static String getRowFromRS(ResultSet rs, int numCol, String table) throws SQLException {
		sbTmp.setLength(0);
		for(int i=1;i<=numCol;i++) {
			sbTmp.append(rs.getString(i));
			sbTmp.append(";");
		}
		return sbTmp.toString();
	}
	
}

package tbrugz.sqldump;

import java.util.*;
import java.sql.*;
import java.io.*;
import java.math.BigDecimal;

import org.apache.log4j.Logger;

import tbrugz.graphml.model.Root;
import tbrugz.graphml.DumpGraphMLModel;
import tbrugz.sqldump.graph.Schema2GraphML;

/*
 * XXX: DDL: grab contents from procedures, triggers and views 
 * TODO: option of data dump with INSERT INTO
 * TODO: generate graphml from schema structure
 * TODOne: column type mapping
 * TODOne: FK constraints at end of schema dump script?
 * TODO: unique constraints?
 * XXXdone: include Grants into SchemaModel?
 * TODO: recursive dump based on FKs
 * TODO: accept list of tables to dump
 * 
 */
public class SQLDataDump {
	
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
	static final String PROP_FROM_DB_ID = "sqldump.fromdbid";
	static final String PROP_TO_DB_ID = "sqldump.todbid";
	static final String PROP_DUMP_WITH_SCHEMA_NAME = "sqldump.dumpwithschemaname";
	
	//column-type-mapping.properties
	//static final String PROP_COLUMN_TYPE_MAPPING_ID = "type.xxx.useprecision";
	
	static final String PROP_DO_TESTS = "sqldump.dotests";
	static final String PROP_DO_DATADUMP = "sqldump.dodatadump";
	static final String PROP_DUMPSCHEMAPATTERN = "sqldump.dumpschemapattern";
	
	static final String PROP_OUTPUTFILE = "sqldump.outputfile";
	
	//properties files filenames
	static final String PROPERTIES_FILENAME = "sqldump.properties";
	static final String COLUMN_TYPE_MAPPING_RESOURCE = "column-type-mapping.properties";
	
	static Logger log = Logger.getLogger(SQLDataDump.class);
	
	FileWriter fos;
	Connection conn;
	
	//model
	List<String> tableNamesForDataDump = new Vector<String>();

	//dumper
	SchemaModelScriptDumper schemaDumper;
	
	Properties papp = new Properties();
	Properties columnTypeMapping = new Properties();
	
	boolean doTests = false, doSchemaDump = false, doDataDump = false;
	boolean doSchemaDumpPKs = false, doSchemaDumpFKs = false, doSchemaDumpFKsAtEnd = false, doSchemaDumpGrants = false;   
	boolean dumpWithSchemaName = false;
	
	void init() throws Exception {
		log.info("init...");
		papp.load(new FileInputStream(PROPERTIES_FILENAME));
		
		//inicializa banco
		Class.forName(papp.getProperty(PROP_DRIVERCLASS));

		Properties p = new Properties();
		p.setProperty("user", papp.getProperty(PROP_USER, ""));
		p.setProperty("password", papp.getProperty(PROP_PASSWD, ""));

		conn = DriverManager.getConnection(papp.getProperty(PROP_URL), p);
		
		//inicializa arquivo de saida
		fos = new FileWriter(papp.getProperty(PROP_OUTPUTFILE)); 
		schemaDumper = new SchemaModelScriptDumper(fos);
		
		//inicializa variaveis controle
		doSchemaDump = papp.getProperty(PROP_DO_SCHEMADUMP, "").equals("true");
		doSchemaDumpPKs = papp.getProperty(PROP_DO_SCHEMADUMP_PKS, "").equals("true");
		doSchemaDumpFKs = papp.getProperty(PROP_DO_SCHEMADUMP_FKS, "").equals("true");
		doSchemaDumpFKsAtEnd = papp.getProperty(PROP_DO_SCHEMADUMP_FKS_ATEND, "").equals("true");
		doSchemaDumpGrants = papp.getProperty(PROP_DO_SCHEMADUMP_GRANTS, "").equals("true");
		dumpWithSchemaName = papp.getProperty(PROP_DUMP_WITH_SCHEMA_NAME, "").equals("true");

		columnTypeMapping.load(SQLDataDump.class.getClassLoader().getResourceAsStream(COLUMN_TYPE_MAPPING_RESOURCE));
		
		schemaDumper.setDumpWithSchemaName(dumpWithSchemaName);
		schemaDumper.dumpPKs = doSchemaDumpPKs;
		schemaDumper.fromDbId = papp.getProperty(PROP_FROM_DB_ID);
		schemaDumper.toDbId = papp.getProperty(PROP_TO_DB_ID);
		schemaDumper.columnTypeMapping = columnTypeMapping;
		schemaDumper.dumpFKsInsideTable = !doSchemaDumpFKsAtEnd;
		
		doTests = papp.getProperty(PROP_DO_TESTS, "").equals("true");
		doDataDump = papp.getProperty(PROP_DO_DATADUMP, "").equals("true"); 
	}

	void end() throws Exception {
		log.info("...done");
		fos.close();
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
			ttype = getTableType(rs.getString("TABLE_TYPE"), tableName);
			
			//defining model
			Table table = new Table();
			table.name = tableName;
			table.type = ttype;
			table.schemaName = schemaName;
			
			try {
				log.debug("getting info from "+(schemaPattern==null?"":schemaPattern+".")+tableName);

				//columns
				ResultSet cols = dbmd.getColumns(null, schemaPattern, tableName, null);
				while(cols.next()) {
					Column c = retrieveColumn(cols);
					table.columns.add(c);
					//String colDesc = getColumnDesc(c, columnTypeMapping, papp.getProperty(PROP_FROM_DB_ID), papp.getProperty(PROP_TO_DB_ID));
				}
				
				//PKs
				if(doSchemaDumpPKs) {
					ResultSet pks = dbmd.getPrimaryKeys(null, schemaPattern, tableName);
					grabSchemaPKs(pks, table);
				}

				//FKs
				if(doSchemaDumpFKs) {
					ResultSet fkrs = dbmd.getImportedKeys(null, schemaPattern, tableName);
					grabSchemaFKs(fkrs, table, schemaModel.foreignKeys);
				}
				
				//GRANTs
				if(doSchemaDumpGrants) {
    				ResultSet grantrs = dbmd.getTablePrivileges(null, schemaPattern, tableName);
    				table.grants = grabSchemaGrants(grantrs, tableName);
				}
				
				cols.close();
			}
			catch(SQLException sqle) {
				log.warn("exception in table: "+tableName+" ["+sqle+"]");
				//sqle.printStackTrace();
				tableNamesForDataDump.remove(tableName);
			}
			
			schemaModel.tables.add(table);
		}
		
		log.debug("tables::["+schemaModel.tables.size()+"]\n"+schemaModel.tables+"\n");
		log.debug("FKs::["+schemaModel.foreignKeys.size()+"]\n"+schemaModel.foreignKeys+"\n");
		return schemaModel;
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

	static String getColumnDesc(Column c, Properties typeMapping, String fromDbId, String toDbId) {
		String colType = c.type;
		if(fromDbId!=null) {
			String ansiColType = typeMapping.getProperty("from."+fromDbId+"."+colType);
			//String newColType = ansiColType; 
			if(ansiColType!=null) { colType = ansiColType; }
		}
		if(toDbId!=null) {	
			String newColType = typeMapping.getProperty("to."+toDbId+"."+colType);
			if(newColType!=null) { colType = newColType; }
		}
		boolean usePrecision = !"false".equals(typeMapping.getProperty("type."+colType+".useprecision"));
		
		return c.name+" "+colType
			+(usePrecision?"("+c.columSize+(c.decimalDigits!=null?","+c.decimalDigits:"")+")":"")
			+(!c.nullable?" not null":"");
	}

	List<Grant> grabSchemaGrants(ResultSet grantrs, String tableName) throws SQLException {
		List<Grant> grantsList = new ArrayList<Grant>();
		while(grantrs.next()) {
			Grant grant = new Grant();
			
			grant.grantee = grantrs.getString("GRANTEE");
			grant.privilege = PrivilegeType.valueOf(grantrs.getString("PRIVILEGE"));
			grant.table = grantrs.getString("TABLE_NAME");
			grant.withGrantOption = "YES".equals(grantrs.getString("IS_GRANTABLE"));
			grantsList.add(grant);
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
				pk = new HashSet<String>();
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
		//dumpRS(fks);
		while(fkrs.next()) {
			//log.debug("FK!!!");
			String fkName = fkrs.getString("FK_NAME");
			if(fkName==null) {
				fkName = "FK_"+count;
				count++;
			}
			log.debug("FK!!! - "+fkName);
			FK fk = fks.get(fkName);
			if(fk==null) {
				fk = new FK();
				fk.name = fkName;
				fks.put(fkName, fk);
			}
			if(fk.pkTable==null) {
				fk.pkTable = fkrs.getString("PKTABLE_NAME");
				fk.fkTable = fkrs.getString("FKTABLE_NAME");
				//fk.pkTable = fkrs.getString("PKTABLE_SCHEM")+"."+fkrs.getString("PKTABLE_NAME");
				//fk.fkTable = fkrs.getString("FKTABLE_SCHEM")+"."+fkrs.getString("FKTABLE_NAME");
				fk.pkTableSchemaName = fkrs.getString("PKTABLE_SCHEM");
				fk.fkTableSchemaName = fkrs.getString("FKTABLE_SCHEM");
			}
			fk.fkColumns.add(fkrs.getString("FKCOLUMN_NAME"));
			fk.pkColumns.add(fkrs.getString("PKCOLUMN_NAME"));
		}
		for(String key: fks.keySet()) {
			foreignKeys.add(fks.get(key));
			/*if(!doSchemaDumpFKsAtEnd) {
				sb.append("\tconstraint "+key+" foreign key ("+join(fks.get(key).fkColumns, ", ")+") references "+fks.get(key).pkTable+" ("+join(fks.get(key).pkColumns, ", ")+"),\n");
			}*/
		}
	}
	
	void dumpData() throws Exception {
		log.info("data dumping...");
		for(String table: tableNamesForDataDump) {
			Statement st = conn.createStatement();
			log.debug("dumping data from table: "+table);
			ResultSet rs = st.executeQuery("select * from \""+table+"\"");
			out("\n[table "+table+"]\n");
			ResultSetMetaData md = rs.getMetaData();
			int numCol = md.getColumnCount();
			while(rs.next()) {
				out(getRowFromRS(rs, numCol, table));
			}
		}
	}
	
	void out(String s) throws IOException {
		//logOutput.info(s);
		fos.write(s+"\n");
	}
	
	void dumpGraph(SchemaModel sm) throws FileNotFoundException {
		Root r = Schema2GraphML.getGraphMlModel(sm);
		DumpGraphMLModel dg = new DumpGraphMLModel();
		dg.dumpModel(r, new PrintStream("output/schema.graphml"));
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
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		SQLDataDump sdd = new SQLDataDump();

		sdd.init();
		
		if(sdd.doTests) {
			sdd.tests();
		}
		if(sdd.doSchemaDump) {
			SchemaModel sm = sdd.grabSchema();
			sdd.schemaDumper.dumpSchema(sm);
			sdd.dumpGraph(sm);
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
	
	static TableType getTableType(String tableType, String tableName) {
		if(tableType.equals("TABLE")) {
			return TableType.TABLE;
		}
		else if(tableType.equals("SYNONYM")) {
			return TableType.SYNONYM;
		}
		else if(tableType.equals("VIEW")) {
			return TableType.VIEW;
		}
		else if(tableType.equals("SYSTEM TABLE")) {
			return TableType.SYSTEM_TABLE;
		}

		log.warn("table "+tableName+" of unknown type: "+tableType);
		return null;
	}

}

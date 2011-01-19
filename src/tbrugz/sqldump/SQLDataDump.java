package tbrugz.sqldump;

import java.util.*;
import java.sql.*;
import java.io.*;
import java.math.BigDecimal;

import org.apache.log4j.Logger;

/*
 * TODO: DDL: grab contents from procedures, triggers and views 
 * TODO: option of data dump with INSERT INTO
 * TODO: generate graphml from schema structure
 * TODO: column type mapping
 * TODOne: FK constraints at end of schema dump script?
 * TODO: unique constraints?
 * TODO: include Grants into SchemaModel?
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
	static final String PROP_COLUMN_TYPE_MAPPING_ID = "sqldump.columntypemappingid";
	
	//column-type-mapping.properties
	//static final String PROP_COLUMN_TYPE_MAPPING_ID = "type.XXX.useprecision";
	
	static final String PROP_DO_TESTS = "sqldump.dotests";
	static final String PROP_DO_DATADUMP = "sqldump.dodatadump";
	static final String PROP_DUMPSCHEMAPATTERN = "sqldump.dumpschemapattern";
	
	static final String PROP_OUTPUTFILE = "sqldump.outputfile";
	
	//properties files filenames
	static final String PROPERTIES_FILENAME = "sqldump.properties";
	static final String COLUMN_TYPE_MAPPING_RESOURCE = "column-type-mapping.properties";
	
	
	/*static final String TABLE = "TABLE";
	static final String TABLE_NAME = "TABLE_NAME";
	static final String TABLE_TYPE = "TABLE_TYPE";*/
	
	//static Logger logOutput = Logger.getLogger(SQLDataDump.class);
	static FileWriter fos;
	static Logger log = Logger.getLogger(SQLDataDump.class);
	
	Connection conn;
	
	//model
	List<String> tableNamesForDump = new Vector<String>();

	Properties papp = new Properties();
	boolean doTests = false, doSchemaDump = false, doDataDump = false;
	boolean doSchemaDumpPKs = false, doSchemaDumpFKs = false, doSchemaDumpFKsAtEnd = false, doSchemaDumpGrants = false;   
	
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
		//	new FileOutputStream();
		
		//inicializa variaveis controle
		doSchemaDump = papp.getProperty(PROP_DO_SCHEMADUMP, "").equals("true");
		doSchemaDumpPKs = papp.getProperty(PROP_DO_SCHEMADUMP_PKS, "").equals("true");
		doSchemaDumpFKs = papp.getProperty(PROP_DO_SCHEMADUMP_FKS, "").equals("true");
		doSchemaDumpFKsAtEnd = papp.getProperty(PROP_DO_SCHEMADUMP_FKS_ATEND, "").equals("true");
		doSchemaDumpGrants = papp.getProperty(PROP_DO_SCHEMADUMP_GRANTS, "").equals("true");
		
		doTests = papp.getProperty(PROP_DO_TESTS, "").equals("true");
		doDataDump = papp.getProperty(PROP_DO_DATADUMP, "").equals("true"); 
	}

	void end() throws Exception {
		log.info("...done");
		fos.close();
		conn.close();
	}
	
	void dumpSchema() throws Exception {
		log.info("schema dump...");
		DatabaseMetaData dbmd = conn.getMetaData();
		
		String schemaPattern = papp.getProperty(PROP_DUMPSCHEMAPATTERN,null);
		log.debug("schema pattern: "+schemaPattern);
		
		ResultSet rs = dbmd.getTables(null, schemaPattern, null, null);
		SchemaModel schemaModel = new SchemaModel();
		
		Properties typeMapping = new Properties();
		typeMapping.load(SQLDataDump.class.getClassLoader().getResourceAsStream(COLUMN_TYPE_MAPPING_RESOURCE));
		System.out.println("typeMapping: "+typeMapping);
		
		//Set<Table> tables = new HashSet<Table>();
		//Set<FK> foreignKeys = new HashSet<FK>();
		
		while(rs.next()) {
			TableType ttype = null;
			String tableName = rs.getString("TABLE_NAME");
			//if(! rs.getString("TABLE_TYPE").equals("SYSTEM TABLE")) {
			tableNamesForDump.add(tableName);
			ttype = getTableType(rs.getString("TABLE_TYPE"), tableName);
			
			//defining model
			Table table = new Table();
			//Map<String, Set<String>> grantsMap = new HashMap<String, Set<String>>();
			table.name = tableName;
			table.type = ttype;
			
			/*
			 * TODOne: PKs/FKs
			 * getPrimaryKeys(String catalog, String schema, String table)
			 * Retrieves a description of the given table's primary key columns.
			 * 
			 * getExportedKeys(String catalog, String schema, String table)
			 * 
			 * getImportedKeys(String catalog, String schema, String table)
			 * Retrieves a description of the primary key columns that are referenced by the given table's foreign key columns (the primary keys imported by a table).
			 * 
			 * getCrossReference(String parentCatalog, String parentSchema, String parentTable, String foreignCatalog, String foreignSchema, String foreignTable)
	         * Retrieves a description of the foreign key columns in the given foreign key table that reference the primary key or the columns representing a unique constraint of the parent table (could be the same or a different table).
	         * 
	         * TODOne: grants
	         * getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern)
	         * Retrieves a description of the access rights for each table available in a catalog.
	         * 
	         * TODO: recursive dump based on FKs
	         * 
	         * Problem with getImportedKeys / getExportedKeys
	         * http://archives.postgresql.org/pgsql-jdbc/2002-01/msg00133.php
			 */
			try {
				log.debug("getting info from "+(schemaPattern==null?"":schemaPattern+".")+tableName);

				StringBuffer sb = new StringBuffer();
				sb.append("--drop table "+tableName+";\n");
				sb.append("create table "+tableName+" ( -- type="+ttype+"\n");
	
				//columns
				ResultSet cols = dbmd.getColumns(null, schemaPattern, tableName, null);
				
				while(cols.next()) {
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
					table.columns.add(c);
					String colType = c.type;
					if(papp.getProperty(PROP_COLUMN_TYPE_MAPPING_ID)!=null) {
						String newColType = typeMapping.getProperty(papp.getProperty(PROP_COLUMN_TYPE_MAPPING_ID)+"."+colType);
						if(newColType!=null) { colType = newColType; }
					}
					boolean usePrecision = !"false".equals(typeMapping.getProperty("type."+colType+".useprecision"));
					sb.append("\t"+c.name+" "+colType
							+(usePrecision?"("+c.columSize+(c.decimalDigits!=null?","+c.decimalDigits:"")+")":"")
							+(!c.nullable?" not null":"")+",\n");
				}
				
				//PKs
				if(doSchemaDumpPKs) {
					ResultSet pks = dbmd.getPrimaryKeys(null, schemaPattern, tableName);
					dumpSchemaPKs(pks, table, sb);
				}
				/*Map<String, Set<String>> tablePK = new HashMap<String, Set<String>>();
				int count=0;
				while(pks.next()) {
					String pkName = pks.getString("PK_NAME");
					if(pkName==null) {
						pkName = "PK_"+count;
						count++;
					}
					Set<String> pk = tablePK.get(pkName);
					if(pk==null) {
						pk = new HashSet<String>();
						tablePK.put(pkName, pk);
					}
					pk.add(pks.getString("COLUMN_NAME"));
				}
				for(String key: tablePK.keySet()) {
					sb.append("\tconstraint "+key+" primary key ("+join(tablePK.get(key), ", ")+"),\n");
					for(String colName: tablePK.get(key)) {
						table.getColumn(colName).pk = true;
					}
				}*/

				//FKs
				if(doSchemaDumpFKs) {
					ResultSet fkrs = dbmd.getImportedKeys(null, schemaPattern, tableName);
					dumpSchemaFKs(fkrs, table, schemaModel.foreignKeys, sb);
				}
				/*Map<String, FK> fks = new HashMap<String, FK>();
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
						fk.pkTable = fkrs.getString("PKTABLE_SCHEM")+"."+fkrs.getString("PKTABLE_NAME");
						fk.fkTable = fkrs.getString("FKTABLE_SCHEM")+"."+fkrs.getString("FKTABLE_NAME");
					}
					fk.fkColumns.add(fkrs.getString("FKCOLUMN_NAME"));
					fk.pkColumns.add(fkrs.getString("PKCOLUMN_NAME"));
				}
				for(String key: fks.keySet()) {
					sb.append("\tconstraint "+key+" foreign key ("+join(fks.get(key).fkColumns, ", ")+") references "+fks.get(key).pkTable+" ("+join(fks.get(key).pkColumns, ", ")+"),\n");
					foreignKeys.add(fks.get(key));
				}*/
				
				sb.delete(sb.length()-2, sb.length());
				sb.append("\n);\n");
				
				//GRANTs
				if(doSchemaDumpGrants) {
    				ResultSet grantrs = dbmd.getTablePrivileges(null, schemaPattern, tableName);
					dumpSchemaGrants(grantrs, tableName, sb);
				}
				
				out(sb.toString());
				cols.close();
			}
			catch(SQLException sqle) {
				log.warn("exception in table: "+tableName+" ["+sqle+"]");
				//sqle.printStackTrace();
				tableNamesForDump.remove(tableName);
			}
			
			//XXXxx: add table to collection
			//table, fks, grantsMap
			schemaModel.tables.add(table);
		}
		
		if(doSchemaDumpFKsAtEnd) {
			StringBuffer sb = new StringBuffer();
			for(FK fk: schemaModel.foreignKeys) {
				sb.append("alter table "+fk.fkTable+"\n\tadd constraint "+fk.name+" foreign key ("+join(fk.fkColumns, ", ")+")\n\treferences "+fk.pkTable+" ("+join(fk.pkColumns, ", ")+");\n\n");
			}
			out(sb.toString());
		}
		
		//XXX dump tables, fks..
		log.info("tables::["+schemaModel.tables.size()+"]\n"+schemaModel.tables+"\n");
		log.info("FKs::["+schemaModel.foreignKeys.size()+"]\n"+schemaModel.foreignKeys+"\n");
	}
	
	void dumpSchemaGrants(ResultSet grantrs, String tableName, StringBuffer sb) throws SQLException {
		//map: grantee -> list<privileges>
		Map<String, Set<String>> grantsMap = new HashMap<String, Set<String>>();
		//ResultSet grantrs = dbmd.getTablePrivileges(null, schemaPattern, tableName);
		while(grantrs.next()) {
			String grantee = grantrs.getString("GRANTEE");
			Set<String> privs = grantsMap.get(grantee);
			if(privs==null) {
				privs = new HashSet<String>();
				grantsMap.put(grantee, privs);
			}
			privs.add(grantrs.getString("PRIVILEGE"));
			/*sb.append("GRANT "+grantrs.getString("PRIVILEGE")
					+" ON "+grantrs.getString("TABLE_NAME")
					+" TO "+grantrs.getString("GRANTEE")
					+("YES".equals(grantrs.getString("IS_GRANTABLE"))?" WITH GRANT OPTION":"")
					+";\n");*/
		}
		for(String grantee: grantsMap.keySet()) {
			sb.append("grant "+join(grantsMap.get(grantee),",")
					+" on "+tableName
					+" to "+grantee
					//+("YES".equals(grantrs.getString("IS_GRANTABLE"))?" WITH GRANT OPTION":"")
					+";\n");
		}
	}

	void dumpSchemaPKs(ResultSet pks, Table table, StringBuffer sb) throws SQLException {
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
			Set<String> pk = tablePK.get(pkName);
			if(pk==null) {
				pk = new HashSet<String>();
				tablePK.put(pkName, pk);
			}
			pk.add(pks.getString("COLUMN_NAME"));
		}
		for(String key: tablePK.keySet()) {
			sb.append("\tconstraint "+key+" primary key ("+join(tablePK.get(key), ", ")+"),\n");
			for(String colName: tablePK.get(key)) {
				table.getColumn(colName).pk = true;
			}
		}
		
	}

	void dumpSchemaFKs(ResultSet fkrs, Table table, Set<FK> foreignKeys, StringBuffer sb) throws SQLException {
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
				fk.pkTable = fkrs.getString("PKTABLE_SCHEM")+"."+fkrs.getString("PKTABLE_NAME");
				fk.fkTable = fkrs.getString("FKTABLE_SCHEM")+"."+fkrs.getString("FKTABLE_NAME");
			}
			fk.fkColumns.add(fkrs.getString("FKCOLUMN_NAME"));
			fk.pkColumns.add(fkrs.getString("PKCOLUMN_NAME"));
		}
		for(String key: fks.keySet()) {
			foreignKeys.add(fks.get(key));
			if(!doSchemaDumpFKsAtEnd) {
				sb.append("\tconstraint "+key+" foreign key ("+join(fks.get(key).fkColumns, ", ")+") references "+fks.get(key).pkTable+" ("+join(fks.get(key).pkColumns, ", ")+"),\n");
			}
		}
	}
	
	void dumpData() throws Exception {
		log.info("data dumping...");
		for(String table: tableNamesForDump) {
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
	
	void testes() throws Exception {
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
			sdd.testes();
		}
		if(sdd.doSchemaDump) {
			sdd.dumpSchema();
		}
		if(sdd.doDataDump) {
			sdd.dumpData();
		}
		
		sdd.end();
	}
	
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
	
	/*
	 * http://snippets.dzone.com/posts/show/91
	 * http://stackoverflow.com/questions/1515437/java-function-for-arrays-like-phps-join
	 */
	public static String join(Collection s, String delimiter) {
		StringBuffer buffer = new StringBuffer();
		Iterator iter = s.iterator();
		while (iter.hasNext()) {
			buffer.append(iter.next());
			if (iter.hasNext()) {
				buffer.append(delimiter);
			}
		}
		return buffer.toString();
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

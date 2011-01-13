package tbrugz.sqldump;

import java.util.*;
import java.sql.*;
import java.io.*;

import org.apache.log4j.Logger;

public class SQLDataDump {
	
	static final String PROP_DRIVERCLASS = "sqldump.driverclass";
	static final String PROP_URL = "sqldump.dburl";
	static final String PROP_USER = "sqldump.user";
	static final String PROP_PASSWD = "sqldump.password";

	static final String PROP_DO_TESTS = "sqldump.dotests";
	static final String PROP_DO_SCHEMADUMP = "sqldump.doschemadump";
	static final String PROP_DO_DATADUMP = "sqldump.dodatadump";
	static final String PROP_DUMPSCHEMAPATTERN = "sqldump.dumpschemapattern";
	
	static final String PROP_OUTPUTFILE = "sqldump.outputfile";
	
	static final String PROPERTIES_FILENAME = "sqldump.properties";
	
	/*static final String TABLE = "TABLE";
	static final String TABLE_NAME = "TABLE_NAME";
	static final String TABLE_TYPE = "TABLE_TYPE";*/
	
	//static Logger logOutput = Logger.getLogger(SQLDataDump.class);
	static FileWriter fos;
	static Logger log = Logger.getLogger(SQLDataDump.class);
	
	Connection conn;
	List<String> tables = new Vector<String>();

	Properties papp = new Properties();
	boolean doTests = false, doSchemaDump = false, doDataDump = false;
	
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
		doTests = papp.getProperty(PROP_DO_TESTS, "").equals("true");
		doSchemaDump = papp.getProperty(PROP_DO_SCHEMADUMP, "").equals("true");
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
		while(rs.next()) {
			String table = rs.getString("TABLE_NAME");
			//if(! rs.getString("TABLE_TYPE").equals("SYSTEM TABLE")) {
			if(rs.getString("TABLE_TYPE").equals("TABLE")) {
				tables.add(table);
			}
			else {
				log.debug("not a real table: "+table+" is a "+rs.getString("TABLE_TYPE"));
			}
			
			/*
			 * TODO: PKs/FKs
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
	         * TODO: grants
	         * getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern)
	         * Retrieves a description of the access rights for each table available in a catalog.
	         * 
	         * TODO: recursive dump based on FKs
	         * 
	         * Problem with getImportedKeys / getExportedKeys
	         * http://archives.postgresql.org/pgsql-jdbc/2002-01/msg00133.php
			 */
			try {
				log.debug("getting info from "+(schemaPattern==null?"":schemaPattern+".")+table);

				ResultSet cols = dbmd.getColumns(null, schemaPattern, table, null);
				StringBuffer sb = new StringBuffer();
				sb.append("create table "+table+" (\n");
	
				//columns
				while(cols.next()) {
					sb.append("\t"+cols.getString("COLUMN_NAME")+" "+cols.getString("TYPE_NAME")+",\n");
				}
				
				//PKs
				Map<String, Set<String>> tablePK = new HashMap<String, Set<String>>();
				ResultSet pks = dbmd.getPrimaryKeys(null, schemaPattern, table);
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
				}

				//FKs
				//List<FK> fks = new ArrayList<FK>();
				//Map<String, List<Set<String>>> tableFKs = new HashMap<String, List<Set<String>>>();
				Map<String, FK> fks = new HashMap<String, FK>();
				ResultSet fkrs = dbmd.getImportedKeys(null, schemaPattern, table);
				count=0;
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
					//List<Set<String>> list = tableFKs.get(fkName);
					if(fk==null) {
						fk = new FK();
						fk.name = fkName;
						//list.add(new HashSet<String>()); //this table keys
						//list.add(new HashSet<String>()); //foreign table keys
						//fk = new HashSet<String>();
						fks.put(fkName, fk);
					}
					//Set<String> fk = list.get(0);
					if(fk.pkTable==null) {
						fk.pkTable = fkrs.getString("PKTABLE_SCHEM")+"."+fkrs.getString("PKTABLE_NAME");
						fk.fkTable = fkrs.getString("FKTABLE_SCHEM")+"."+fkrs.getString("FKTABLE_NAME");
					}
					fk.fkColumns.add(fkrs.getString("FKCOLUMN_NAME"));
					fk.pkColumns.add(fkrs.getString("PKCOLUMN_NAME"));
				}
				for(String key: fks.keySet()) {
					sb.append("\tconstraint "+key+" foreign key ("+join(fks.get(key).fkColumns, ", ")+") references "+fks.get(key).pkTable+" ("+join(fks.get(key).pkColumns, ", ")+"),\n");
				}
				
				sb.append(");\n");
				
				//GRANTs
				//map: grantee -> list<privileges>
				Map<String, Set<String>> privilegeMap = new HashMap<String, Set<String>>();
				ResultSet grantrs = dbmd.getTablePrivileges(null, schemaPattern, table);
				while(grantrs.next()) {
					String grantee = grantrs.getString("GRANTEE");
					Set<String> privs = privilegeMap.get(grantee);
					if(privs==null) {
						privs = new HashSet<String>();
						privilegeMap.put(grantee, privs);
					}
					privs.add(grantrs.getString("PRIVILEGE"));
					/*sb.append("GRANT "+grantrs.getString("PRIVILEGE")
							+" ON "+grantrs.getString("TABLE_NAME")
							+" TO "+grantrs.getString("GRANTEE")
							+("YES".equals(grantrs.getString("IS_GRANTABLE"))?" WITH GRANT OPTION":"")
							+";\n");*/
				}
				for(String grantee: privilegeMap.keySet()) {
					sb.append("grant "+join(privilegeMap.get(grantee),",")
							+" on "+table
							+" to "+grantee
							//+("YES".equals(grantrs.getString("IS_GRANTABLE"))?" WITH GRANT OPTION":"")
							+";\n");
				}
				
				out(sb.toString());
				cols.close();
			}
			catch(SQLException sqle) {
				log.warn("exception in table: "+table+" ["+sqle+"]");
				//sqle.printStackTrace();
				tables.remove(table);
			}
			
			//System.gc();
			//Thread.yield();
		}
		
	}
	
	void dumpData() throws Exception {
		log.info("fazendo dump de dados");
		for(String table: tables) {
			Statement st = conn.createStatement();
			log.debug("fazendo dump dos dados de "+table);
			ResultSet rs = st.executeQuery("select * from \""+table+"\"");
			out("\n[tabela "+table+"]\n");
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
		log.info("testes...");

		DatabaseMetaData dbmd = conn.getMetaData();

		log.info("testes: catalogs...");
		dumpRS(dbmd.getCatalogs());

		log.info("testes: table types...");
		dumpRS(dbmd.getTableTypes());

		//log.info("testes: tables...");
		//dumpRS(dbmd.getTables(null, null, null, null));

		log.info("testes: fks...");
		//dumpRS(dbmd.getImportedKeys(null, "schema", "table"));

		//log.info("testes: grants...");
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

}

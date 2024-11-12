package tbrugz.sqldump;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import tbrugz.sqldump.dbmd.DBMSFeatures;
import tbrugz.sqldump.dbmodel.Grant;
import tbrugz.sqldump.dbmodel.PrivilegeType;
import tbrugz.sqldump.dbmodel.SchemaModel;
import tbrugz.sqldump.def.DBMSResources;
import tbrugz.sqldump.def.Defs;
import tbrugz.sqldump.def.ProcessingException;
import tbrugz.sqldump.processors.SQLDialectTransformer;
import tbrugz.sqldump.util.IOUtil;
import tbrugz.sqldump.util.SQLIdentifierDecorator;

public class ScriptDumperTest {

	final static String OUTDIR = "work/output/ScriptDumperTest";
	
	final static String T = "true";
	final static String F = "false";
	
	final static String DB_MYSQL = "mysql";
	final static String DB_ORACLE = "oracle";
	
	SchemaModel sm = null;
	SchemaModelScriptDumper sd = new SchemaModelScriptDumper();
	Properties p = new Properties();
	
	@Before
	public void before() {
		DBMSResources.instance();
		
		{
			JAXBSchemaXMLSerializer jss = new JAXBSchemaXMLSerializer();
			Properties p = new Properties();
			p.setProperty(JAXBSchemaXMLSerializer.XMLSERIALIZATION_JAXB_DEFAULT_PREFIX+JAXBSchemaXMLSerializer.PROP_XMLSERIALIZATION_JAXB_INRESOURCE,
					"/tbrugz/sqldump/processors/empdept.jaxb.xml");
			jss.setProperties(p);
			sm = jss.grabSchema();
		}

		p.setProperty(SchemaModelScriptDumper.PROP_SCHEMADUMP_QUOTEALLSQLIDENTIFIERS, T);
		p.setProperty(SchemaModelScriptDumper.PROP_SCHEMADUMP_DUMPSCRIPTCOMMENTS, F);
	}
	
	@Test
	public void dumpMysql() {
		String file = OUTDIR+"/empdept-"+DB_MYSQL+".sql";
		p.setProperty(SchemaModelScriptDumper.PROP_SCHEMADUMP_OUTPUT_FILE_PATTERN, file);
		p.setProperty(Defs.PROP_TO_DB_ID, DB_MYSQL);
		
		//DBMSResources.instance().updateDbId(DB_MYSQL);
		//DBMSResources.instance().setup(p);
		//ColTypeUtil.setProperties(p);
		//SQLUtils.setProperties(p);
		//Column.ColTypeUtil.setDbId(DB_MYSQL);
		DBMSFeatures feat = DBMSResources.instance().getSpecificFeatures(DB_MYSQL);
		SQLIdentifierDecorator.dumpIdentifierQuoteString = feat.getIdentifierQuoteString();
		
		sd.setProperties(p);
		sd.dumpSchema(sm);
		
		String sql = IOUtil.readFromFilename(file);
		String expected = "create table `DEPT` (\n	`ID` INTEGER not null,\n	`NAME` VARCHAR(100),\n	`PARENT_ID` INTEGER,\n	constraint `DEPT_PK` primary key (`ID`)\n);";
		Assert.assertEquals(expected, sql.substring(0, expected.length()));
	}

	@Test
	public void dumpOracle() {
		String file = OUTDIR+"/empdept-"+DB_ORACLE+".sql";
		p.setProperty(SchemaModelScriptDumper.PROP_SCHEMADUMP_OUTPUT_FILE_PATTERN, file);
		p.setProperty(Defs.PROP_TO_DB_ID, DB_ORACLE);

		//DBMSResources.instance().updateDbId(DB_ORACLE);
		
		sd.setProperties(p);
		sd.dumpSchema(sm);
		
		String sql = IOUtil.readFromFilename(file);
		String expected = "create table \"DEPT\" (\n	\"ID\" INTEGER not null,\n	\"NAME\" VARCHAR(100),\n	\"PARENT_ID\" INTEGER,\n	constraint \"DEPT_PK\" primary key (\"ID\")\n);";
		Assert.assertEquals(expected, sql.substring(0, expected.length()));
	}

	@Test(expected=ProcessingException.class)
	public void dumpUnknown() {
		p.setProperty(SchemaModelScriptDumper.PROP_SCHEMADUMP_OUTPUT_FILE_PATTERN, OUTDIR+"/empdept-zzz.sql");
		p.setProperty(Defs.PROP_TO_DB_ID, "zzz");
		
		sd.setProperties(p);
		sd.dumpSchema(sm);
	}
	
	@Test
	public void dumpMysqlDialect() {
		String file = OUTDIR+"/empdept-"+DB_MYSQL+".sql";
		p.setProperty(SchemaModelScriptDumper.PROP_SCHEMADUMP_OUTPUT_FILE_PATTERN, file);
		p.setProperty(Defs.PROP_TO_DB_ID, DB_MYSQL);
		//DBMSResources.instance().updateDbId(DB_MYSQL);
		
		SQLDialectTransformer dt = new SQLDialectTransformer();
		dt.setSchemaModel(sm);
		dt.setProperties(p);
		dt.process();
		
		sd.setProperties(p);
		sd.dumpSchema(sm);
		
		String sql = IOUtil.readFromFilename(file);
		String expected = "create table `DEPT` (\n	`ID` INTEGER not null,\n	`NAME` VARCHAR(100),\n	`PARENT_ID` INTEGER,\n	constraint `DEPT_PK` primary key (`ID`)\n);";
		Assert.assertEquals(expected, sql.substring(0, expected.length()));
	}

	@Test
	public void dumpOracleDialect() {
		String file = OUTDIR+"/empdept-"+DB_ORACLE+".sql";
		p.setProperty(SchemaModelScriptDumper.PROP_SCHEMADUMP_OUTPUT_FILE_PATTERN, file);
		p.setProperty(Defs.PROP_TO_DB_ID, DB_ORACLE);
		//DBMSResources.instance().updateDbId(DB_ORACLE);
		
		SQLDialectTransformer dt = new SQLDialectTransformer();
		dt.setSchemaModel(sm);
		dt.setProperties(p);
		dt.process();
		
		sd.setProperties(p);
		sd.dumpSchema(sm);

		String sql = IOUtil.readFromFilename(file);
		String expected = "create table \"DEPT\" (\n	\"ID\" INTEGER not null,\n	\"NAME\" VARCHAR2(100),\n	\"PARENT_ID\" INTEGER,\n	constraint \"DEPT_PK\" primary key (\"ID\")\n);";
		Assert.assertEquals(expected, sql.substring(0, expected.length()));
	}

	@Test
	public void dumpMysqlFull() throws Exception {
		String file = OUTDIR+"/empdept-mysql-full.sql";
		String[] args1 = { "-propfile=src/test/resources/tbrugz/sqldump/sqld-mysql.properties" };
		SQLDump.main(args1);
		
		String sql = IOUtil.readFromFilename(file);
		String expected = "create table `DEPT` (\n	`ID` INTEGER not null,\n	`NAME` VARCHAR(100),\n	`PARENT_ID` INTEGER,\n	constraint `DEPT_PK` primary key (`ID`)\n);";
		Assert.assertEquals(expected, sql.substring(0, sql.indexOf(";")+1));
	}

	@Test
	public void dumpSelectPrivilege() throws Exception {
		Grant gr = new Grant("GRANTOR", PrivilegeType.SELECT, "GRANTEE");
		List<Grant> grants = new ArrayList<>();
		grants.add(gr);
		Set<String> privs = new TreeSet<>();
		privs.add("SELECT");
		
		String sql = SchemaModelScriptDumper.compactGrantDump(grants, "TABLE_X", privs);
		Assert.assertEquals("grant SELECT on TABLE_X to GRANTEE;", sql.trim());
	}

	@Test
	public void dumpSelectPrivilegeWithColumn() throws Exception {
		Grant gr = new Grant("GRANTOR", Arrays.asList("COL_1"), PrivilegeType.SELECT, "GRANTEE", false);
		List<Grant> grants = new ArrayList<>();
		grants.add(gr);
		Set<String> privs = new TreeSet<>();
		privs.add("SELECT");
		
		String sql = SchemaModelScriptDumper.compactGrantDump(grants, "TABLE_X", privs);
		Assert.assertEquals("grant SELECT (COL_1) on TABLE_X to GRANTEE;", sql.trim());
	}

	@Test
	public void dumpSelectPrivilegeWithoutColumn() throws Exception {
		Grant gr = new Grant("GRANTOR", Arrays.asList("COL_1"), PrivilegeType.DELETE, "GRANTEE", false);
		List<Grant> grants = new ArrayList<>();
		grants.add(gr);
		Set<String> privs = new TreeSet<>();
		privs.add("DELETE");
		
		String sql = SchemaModelScriptDumper.compactGrantDump(grants, "TABLE_X", privs);
		Assert.assertEquals("grant DELETE on TABLE_X to GRANTEE;", sql.trim());
	}
	
}

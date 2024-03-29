package tbrugz.sqldiff.model;

import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import tbrugz.sqldiff.model.ColumnDiff.TempColumnAlterStrategy;
import tbrugz.sqldump.dbmodel.Column;
import tbrugz.sqldump.dbmodel.Table;
import tbrugz.sqldump.def.DBMSResources;
import tbrugz.sqldump.util.SQLIdentifierDecorator;
import tbrugz.sqldump.util.Utils;

public class ColumnDiffTest {
	
	static Table table = new Table();
	
	@BeforeClass
	public static void beforeClass() {
		table.setName("a");
		ColumnDiff.addComments = false;
		SQLIdentifierDecorator.dumpQuoteAll = false;
	}
	
	@Before
	public void before() {
		updateDbId(null);
		ColumnDiff.useTempColumnStrategy = TempColumnAlterStrategy.NEVER;
	}
	
	void updateDbId(String s) {
		//DBMSResources.instance().updateDbId(s);
		ColumnDiff.updateFeatures(DBMSResources.instance().getSpecificFeatures(s));
	}
	
	@Test
	public void testAdd() {
		Column c2 = newColumn("cnew","varchar",20);
		ColumnDiff cd = new ColumnDiff(ChangeType.ADD, table, null, c2);
		String diff = cd.getDiff();
		out(diff);
		Assert.assertEquals("alter table a add column cnew varchar(20)", diff);
	}

	@Test
	public void testDrop() {
		Column c1 = newColumn("cold","varchar",10);
		ColumnDiff cd = new ColumnDiff(ChangeType.DROP, table, c1, null);
		String diff = cd.getDiff();
		out(diff);
		Assert.assertEquals("alter table a drop column cold", diff);
	}
	
	@Test
	public void testRename() {
		Column c1 = newColumn("c1","varchar",10);
		Column c2 = newColumn("c2","varchar",20);
		ColumnDiff cd = new ColumnDiff(ChangeType.RENAME, table, c1, c2);
		String diff = cd.getDiff();
		out(diff);
		Assert.assertEquals("alter table a rename column c1 to c2", diff);
	}

	@Test
	public void testRenameH2() {
		updateDbId("h2");
		
		Column c1 = newColumn("c1","varchar",10);
		Column c2 = newColumn("c2","varchar",20);
		ColumnDiff cd = new ColumnDiff(ChangeType.RENAME, table, c1, c2);
		String diff = cd.getDiff();
		out(diff);
		Assert.assertEquals("alter table a alter column c1 rename to c2", diff);
	}
	
	@Test
	public void testAlterH2DataType() {
		updateDbId("h2");
		
		Column c1 = newColumn("cx","varchar",10);
		Column c2 = newColumn("cx","varchar",20);
		ColumnDiff cd = new ColumnDiff(ChangeType.ALTER, table, c1, c2);
		String diff = cd.getDiff();
		out(diff);
		Assert.assertEquals("alter table a alter column cx varchar(20)", diff);
	}

	@Test
	public void testAlterH2SetNullNotNull() {
		updateDbId("h2");
		
		Column c1 = newColumn("cx","varchar",10, false);
		Column c2 = newColumn("cx","varchar",10, true);
		
		{
			ColumnDiff cd = new ColumnDiff(ChangeType.ALTER, table, c1, c2);
			String diff = cd.getDiff();
			out(diff);
			Assert.assertEquals("alter table a alter column cx set null", diff);
		}
		{
			ColumnDiff cd = new ColumnDiff(ChangeType.ALTER, table, c2, c1);
			String diff = cd.getDiff();
			out(diff);
			Assert.assertEquals("alter table a alter column cx set not null", diff);
		}
	}
	
	@Test
	public void testAlterH2SetDefaultNull() {
		updateDbId("h2");
		
		Column c1 = newColumn("cx","varchar",10, true, "Y");
		Column c2 = newColumn("cx","varchar",10, true, null);
		
		{
			ColumnDiff cd = new ColumnDiff(ChangeType.ALTER, table, c1, c2);
			String diff = cd.getDiff();
			out(diff);
			Assert.assertEquals("alter table a alter column cx set default null", diff);
		}
	}

	@Test
	public void testAlterH2SetDefaultY() {
		updateDbId("h2");
		
		Column c1 = newColumn("cx","varchar",10, true, "'Y'");
		Column c2 = newColumn("cx","varchar",10, true, null);
		{
			ColumnDiff cd = new ColumnDiff(ChangeType.ALTER, table, c2, c1);
			String diff = cd.getDiff();
			out(diff);
			Assert.assertEquals("alter table a alter column cx set default 'Y'", diff);
		}
		
	}

	@Test
	public void testAlterOracleSetDefaultY() {
		updateDbId("oracle");
		
		Column c1 = newColumn("cx","varchar",10, true, "'Y'");
		Column c2 = newColumn("cx","varchar",10, true, null);
		
		ColumnDiff cd = new ColumnDiff(ChangeType.ALTER, table, c2, c1);
		String diff = cd.getDiff();
		out(diff);
		Assert.assertEquals("alter table a modify cx default 'Y'", diff);
	}

	String alterSimple = "alter table a alter column cx varchar(20)";
	String[] alterWithTempColList = {
			"alter table a rename column cx to cx_TMP",
			"alter table a add column cx varchar(20)",
			"update a set cx = cx_TMP",
			"alter table a drop column cx_TMP"
	};
	String alterWithTempCol = Utils.join(Arrays.asList(alterWithTempColList), ";\n");
	String alterWithTempColInv = null;
	{
		String[] alterWithTempColListInv = Arrays.copyOf(alterWithTempColList, alterWithTempColList.length);
		alterWithTempColListInv[1] = "alter table a add column cx varchar(10)";
		alterWithTempColInv = Utils.join(Arrays.asList(alterWithTempColListInv), ";\n");
	}
	
	@Test
	public void testAlter() {
		Column c1 = newColumn("cx","varchar",10);
		Column c2 = newColumn("cx","varchar",20);
		ColumnDiff cd = new ColumnDiff(ChangeType.ALTER, table, c1, c2);
		String diff = cd.getDiff();
		out(diff);
		Assert.assertEquals(alterSimple, diff);
	}
	
	@Test
	public void testAlterTempColAlways() {
		ColumnDiff.useTempColumnStrategy = TempColumnAlterStrategy.ALWAYS;
		Column c1 = newColumn("cx","varchar",10);
		Column c2 = newColumn("cx","varchar",20);
		ColumnDiff cd = new ColumnDiff(ChangeType.ALTER, table, c1, c2);
		String diff = cd.getDiff();
		out(diff);
		Assert.assertEquals(alterWithTempCol, diff);
	}

	@Test
	public void testAlterTempColH2After() {
		updateDbId("h2");
		ColumnDiff.useTempColumnStrategy = TempColumnAlterStrategy.ALWAYS;
		
		Column c1 = newColumn("cx","varchar",10);
		Column c2 = newColumn("cx","varchar",20);
		ColumnDiff cd = new ColumnDiff(ChangeType.ALTER, table, c1, c2);
		String diff = cd.getDiff();
		
		out(diff);
		String[] alterWithTempColList = {
				"alter table a alter column cx rename to cx_TMP",
				"alter table a add column cx varchar(20) after cx_TMP",
				"update a set cx = cx_TMP",
				"alter table a drop column cx_TMP"
		};
		String expected = Utils.join(Arrays.asList(alterWithTempColList), ";\n");
		Assert.assertEquals(expected, diff);
	}
	
	@Test
	public void testAlterTempColAlwaysWithList() {
		ColumnDiff.useTempColumnStrategy = TempColumnAlterStrategy.ALWAYS;
		Column c1 = newColumn("cx","varchar",10);
		Column c2 = newColumn("cx","varchar",20);
		ColumnDiff cd = new ColumnDiff(ChangeType.ALTER, table, c1, c2);
		List<String> diff = cd.getDiffList();
		Assert.assertEquals(alterWithTempColList[0], diff.get(0));
		Assert.assertEquals(alterWithTempColList[1], diff.get(1));
		Assert.assertEquals(alterWithTempColList[2], diff.get(2));
		Assert.assertEquals(alterWithTempColList[3], diff.get(3));
	}
	
	@Test
	public void testAlterTempColOther() {
		Column c1 = newColumn("cx","varchar",10);
		Column c2 = newColumn("cx","varchar",20);

		ColumnDiff.useTempColumnStrategy = TempColumnAlterStrategy.NEWPRECISIONSMALLER;
		ColumnDiff cd1 = new ColumnDiff(ChangeType.ALTER, table, c1, c2);
		Assert.assertEquals(alterSimple, cd1.getDiff());

		ColumnDiff cd1i = new ColumnDiff(ChangeType.ALTER, table, c2, c1);
		Assert.assertEquals(alterWithTempColInv, cd1i.getDiff());
		
		ColumnDiff.useTempColumnStrategy = TempColumnAlterStrategy.TYPESDIFFER;
		ColumnDiff cd2 = new ColumnDiff(ChangeType.ALTER, table, c1, c2);
		Assert.assertEquals(alterSimple, cd2.getDiff());
	}

	@Test
	public void testAlterOracle() {
		updateDbId("oracle");
		
		Column c1 = newColumn("cx","varchar",10);
		Column c2 = newColumn("cx","varchar",20);
		ColumnDiff cd = new ColumnDiff(ChangeType.ALTER, table, c1, c2);
		String diff = cd.getDiff();
		out(diff);
		Assert.assertEquals("alter table a modify cx varchar(20)", diff);
	}

	@Test
	public void testAlterPgsqlDataType() {
		updateDbId("pgsql");
		
		Column c1 = newColumn("cx","varchar",10);
		Column c2 = newColumn("cx","varchar",20);
		ColumnDiff cd = new ColumnDiff(ChangeType.ALTER, table, c1, c2);
		String diff = cd.getDiff();
		out(diff);
		Assert.assertEquals("alter table a alter column cx set data type varchar(20)", diff);
	}

	@Test
	public void testAlterPgsqlNullable() {
		updateDbId("pgsql");
		
		Column c1 = newColumn("cx","varchar",10);
		Column c2 = newColumn("cx","varchar",10, false);
		ColumnDiff cd = new ColumnDiff(ChangeType.ALTER, table, c1, c2);
		String diff = cd.getDiff();
		out(diff);
		Assert.assertEquals("alter table a alter column cx set not null", diff);
	}

	@Test
	public void testAlterPgsqlNotNullable() {
		updateDbId("pgsql");
		
		Column c1 = newColumn("cx","varchar",10, false);
		Column c2 = newColumn("cx","varchar",10);
		ColumnDiff cd = new ColumnDiff(ChangeType.ALTER, table, c1, c2);
		String diff = cd.getDiff();
		out(diff);
		Assert.assertEquals("alter table a alter column cx drop not null", diff);
	}
	
	public static Column newColumn(String name, String type, int precision) {
		return newColumn(name, type, precision, true);
	}
	
	public static Column newColumn(String name, String type, int precision, boolean nullable) {
		return newColumn(name, type, precision, nullable, null);
	}
	
	public static Column newColumn(String name, String type, int precision, boolean nullable, String defaultValue) {
		Column c = new Column();
		c.setName(name);
		c.setType(type);
		c.setColumnSize(precision);
		c.setNullable(nullable);
		c.setDefaultValue(defaultValue);
		return c;
	}
	
	void out(String s) {
		System.out.println(s);
		System.out.println();
	}
}

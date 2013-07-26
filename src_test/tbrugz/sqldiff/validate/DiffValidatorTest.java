package tbrugz.sqldiff.validate;

import org.junit.BeforeClass;
import org.junit.Test;

import tbrugz.sqldiff.model.ChangeType;
import tbrugz.sqldiff.model.ColumnDiff;
import tbrugz.sqldiff.model.ColumnDiffTest;
import tbrugz.sqldiff.util.DiffUtil;
import tbrugz.sqldump.dbmodel.Column;
import tbrugz.sqldump.dbmodel.SchemaModel;
import tbrugz.sqldump.dbmodel.Table;

public class DiffValidatorTest {

	static SchemaModel sm = new SchemaModel();
	static Table table = new Table();
	
	@BeforeClass
	public static void beforeClass() {
		Column c = ColumnDiffTest.newColumn("c","varchar",10);
		table.setName("t");
		table.setColumns(DiffUtil.singleElemList(c));
		sm.getTables().add(table);
	}
	
	@Test
	public void testColumnDiffAddOk() {
		Column c2 = ColumnDiffTest.newColumn("cnew","varchar",20);
		ColumnDiff cd = new ColumnDiff(ChangeType.ADD, table, null, c2);
		DiffValidator dv = new DiffValidator(sm);
		dv.validateDiff(cd);
	}

	@Test(expected=IncompatibleChangeException.class)
	public void testColumnDiffAddError() {
		Column c2 = ColumnDiffTest.newColumn("c","varchar",20);
		ColumnDiff cd = new ColumnDiff(ChangeType.ADD, table, null, c2);
		DiffValidator dv = new DiffValidator(sm);
		dv.validateDiff(cd);
	}

	@Test
	public void testColumnDiffDropOk() {
		Column c2 = ColumnDiffTest.newColumn("c","varchar",20);
		ColumnDiff cd = new ColumnDiff(ChangeType.DROP, table, c2, null);
		DiffValidator dv = new DiffValidator(sm);
		dv.validateDiff(cd);
	}

	@Test(expected=IncompatibleChangeException.class)
	public void testColumnDiffDropError() {
		Column c2 = ColumnDiffTest.newColumn("cnew","varchar",20);
		ColumnDiff cd = new ColumnDiff(ChangeType.DROP, table, c2, null);
		DiffValidator dv = new DiffValidator(sm);
		dv.validateDiff(cd);
	}
}

package tbrugz.sqldiff.util;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import tbrugz.sqldiff.ConflictingChangesException;
import tbrugz.sqldiff.RenameDetector;
import tbrugz.sqldiff.model.ChangeType;
import tbrugz.sqldiff.model.ColumnDiff;
import tbrugz.sqldiff.model.ColumnDiffTest;
import tbrugz.sqldiff.model.DBIdentifiableDiff;
import tbrugz.sqldiff.model.TableDiff;
import tbrugz.sqldump.dbmd.DBMSFeatures;
import tbrugz.sqldump.dbmodel.Column;
import tbrugz.sqldump.dbmodel.Constraint;
import tbrugz.sqldump.dbmodel.Constraint.ConstraintType;
import tbrugz.sqldump.dbmodel.Index;
import tbrugz.sqldump.dbmodel.Table;
import tbrugz.sqldump.def.DBMSResources;

public class RenameDetectorTest {

	Table t1 = new Table();
	Table t2 = new Table();
	
	Column c1 = ColumnDiffTest.newColumn("c", "varchar", 10);
	Column c2 = ColumnDiffTest.newColumn("c", "varchar", 10);
	
	@BeforeClass
	public static void setupClass() {
		DBMSFeatures feat = DBMSResources.instance().getSpecificFeatures("h2");
		ColumnDiff.updateFeatures(feat);
	}
	
	@Before
	public void setup() {
		t1.setName("t");
		t1.getColumns().add(ColumnDiffTest.newColumn("c1", "varchar", 10));
		t1.getColumns().add(ColumnDiffTest.newColumn("c2", "varchar", 10));

		t2.setName("t");
		t2.getColumns().add(ColumnDiffTest.newColumn("c2", "varchar", 10));
		t2.getColumns().add(ColumnDiffTest.newColumn("c1", "varchar", 10));
	}

	@Test
	public void testDoRename1() {
		TableDiff td1 = new TableDiff(ChangeType.ADD, t1);
		TableDiff td2 = new TableDiff(ChangeType.DROP, t1);
		List<TableDiff> lt = new ArrayList<TableDiff>();
		lt.add(td1); lt.add(td2);
		
		RenameDetector.detectAndDoTableRenames(lt, 0.5);
		Assert.assertEquals(1, lt.size());
		Assert.assertEquals(ChangeType.RENAME, lt.get(0).getChangeType());
	}

	@Test
	public void testDoRename2() {
		TableDiff td1 = new TableDiff(ChangeType.ADD, t1);
		TableDiff td2 = new TableDiff(ChangeType.DROP, t2);
		List<TableDiff> lt = new ArrayList<TableDiff>();
		lt.add(td1); lt.add(td2);
		
		RenameDetector.detectAndDoTableRenames(lt, 0.5);
		Assert.assertEquals(1, lt.size());
		Assert.assertEquals(ChangeType.RENAME, lt.get(0).getChangeType());
	}

	@Test(expected=ConflictingChangesException.class)
	public void testErrorRename1() {
		TableDiff td1 = new TableDiff(ChangeType.ADD, t1);
		TableDiff td2 = new TableDiff(ChangeType.DROP, t2);
		TableDiff td3 = new TableDiff(ChangeType.DROP, t1);
		List<TableDiff> lt = new ArrayList<TableDiff>();
		lt.add(td1); lt.add(td2); lt.add(td3);
		
		// which to rename? which to drop?
		RenameDetector.detectAndDoTableRenames(lt, 0.5); //0.9?
	}

	@Test(expected=ConflictingChangesException.class)
	public void testErrorRename2() {
		TableDiff td1 = new TableDiff(ChangeType.ADD, t1);
		TableDiff td2 = new TableDiff(ChangeType.DROP, t2);
		TableDiff td3 = new TableDiff(ChangeType.DROP, t1);
		TableDiff td4 = new TableDiff(ChangeType.ADD, t2);
		List<TableDiff> lt = new ArrayList<TableDiff>();
		lt.add(td1); lt.add(td2); lt.add(td3); lt.add(td4);
		
		RenameDetector.detectAndDoTableRenames(lt, 0.5);
	}

	@Test
	public void testDoRenameColumn1() {
		ColumnDiff cd1 = new ColumnDiff(ChangeType.DROP, t1, c1, null);
		ColumnDiff cd2 = new ColumnDiff(ChangeType.ADD, t1, null, c2);
		List<ColumnDiff> lt = new ArrayList<ColumnDiff>();
		lt.add(cd1); lt.add(cd2);
		
		RenameDetector.detectAndDoColumnRenames(lt, 0.5);
		Assert.assertEquals(1, lt.size());
		Assert.assertEquals(ChangeType.RENAME, lt.get(0).getChangeType());
	}
	
	@Test(expected=ConflictingChangesException.class)
	public void testErrorRenameColumn1() {
		ColumnDiff cd1 = new ColumnDiff(ChangeType.DROP, t1, c1, null);
		ColumnDiff cd2 = new ColumnDiff(ChangeType.ADD, t1, null, c2);
		ColumnDiff cd3 = new ColumnDiff(ChangeType.ADD, t1, null, c2);
		List<ColumnDiff> lt = new ArrayList<ColumnDiff>();
		lt.add(cd1); lt.add(cd2); lt.add(cd3);
		
		RenameDetector.detectAndDoColumnRenames(lt, 0.5);
		Assert.assertEquals(1, lt.size());
		Assert.assertEquals(ChangeType.RENAME, lt.get(0).getChangeType());
	}

	@Test
	public void testDoRenameIndex() {
		Index i1 = new Index();
		i1.setTableName("table1");
		i1.setName("idx1a");
		i1.getColumns().add("col1");
		Index i2 = new Index();
		i2.setTableName("table1");
		i2.setName("idx1b");
		i2.getColumns().add("col1");

		DBIdentifiableDiff dbd1 = new DBIdentifiableDiff(ChangeType.DROP, i1, null, null);
		DBIdentifiableDiff dbd2 = new DBIdentifiableDiff(ChangeType.ADD, null, i2, null);
		List<DBIdentifiableDiff> lt = new ArrayList<DBIdentifiableDiff>();
		lt.add(dbd1); lt.add(dbd2);
		
		RenameDetector.detectAndDoIndexRenames(lt, 0.5);
		Assert.assertEquals(1, lt.size());
		Assert.assertEquals(ChangeType.RENAME, lt.get(0).getChangeType());
	}

	@Test
	public void testDoRenamePk() {
		Constraint c1 = new Constraint();
		c1.setName("cons1a");
		c1.getUniqueColumns().add("c1");
		c1.setType(ConstraintType.PK);

		Constraint c2 = new Constraint();
		c2.setName("cons2a");
		c2.getUniqueColumns().add("c1");
		c2.setType(ConstraintType.PK);

		DBIdentifiableDiff dbd1 = new DBIdentifiableDiff(ChangeType.DROP, c1, null, "table1");
		
		{
			DBIdentifiableDiff dbd2 = new DBIdentifiableDiff(ChangeType.ADD, null, c2, "table1");
			List<DBIdentifiableDiff> l = new ArrayList<DBIdentifiableDiff>();
			l.add(dbd1); l.add(dbd2);
			
			RenameDetector.detectAndDoConstraintRenames(l, 0.5);
			Assert.assertEquals(1, l.size());
			Assert.assertEquals(ChangeType.RENAME, l.get(0).getChangeType());
		}
		{
			DBIdentifiableDiff dbd3 = new DBIdentifiableDiff(ChangeType.ADD, null, c2, "table3");
			List<DBIdentifiableDiff> l2 = new ArrayList<DBIdentifiableDiff>();
			l2.add(dbd1); l2.add(dbd3);
			RenameDetector.detectAndDoConstraintRenames(l2, 0.5);
			Assert.assertEquals(2, l2.size());
			Assert.assertEquals(ChangeType.DROP, l2.get(0).getChangeType());
			Assert.assertEquals(ChangeType.ADD, l2.get(1).getChangeType());
		}
	}
	
}

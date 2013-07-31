package tbrugz.sqldiff.util;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import tbrugz.sqldiff.ConflictingChangesException;
import tbrugz.sqldiff.model.ChangeType;
import tbrugz.sqldiff.model.ColumnDiff;
import tbrugz.sqldiff.model.ColumnDiffTest;
import tbrugz.sqldiff.model.TableDiff;
import tbrugz.sqldump.dbmodel.Column;
import tbrugz.sqldump.dbmodel.Table;

public class RenameDetectorTest {

	Table t1 = new Table();
	Table t2 = new Table();
	
	Column c1 = ColumnDiffTest.newColumn("c", "varchar", 10);
	Column c2 = ColumnDiffTest.newColumn("c", "varchar", 10);
	
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
}

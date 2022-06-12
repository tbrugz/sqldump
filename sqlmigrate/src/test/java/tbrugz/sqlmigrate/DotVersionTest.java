package tbrugz.sqlmigrate;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

public class DotVersionTest {

	List<Integer> toIntegerArray(int[] ints) {
		List<Integer> ret = new ArrayList<>();
		for(int i: ints) {
			ret.add(i);
		}
		return ret;
	}
	
	@Test
	public void getIntListVersion() {
		int[] expected = {1, 111, 123};
		List<Integer> l = DotVersion.getIntListVersion("1.111.123");
		Assert.assertEquals(toIntegerArray(expected), l);
	}
	
	@Test
	public void getIntListVersion2() {
		int[] expected = {1, 222, 123};
		List<Integer> l = DotVersion.getIntListVersion("1-222.123");
		Assert.assertEquals(toIntegerArray(expected), l);
	}

	@Test
	public void getIntListVersion4() {
		int[] expected = {1, 224, 123};
		List<Integer> l = DotVersion.getIntListVersion("1.224.0123");
		Assert.assertEquals(toIntegerArray(expected), l);
	}

	@Test(expected = IllegalArgumentException.class)
	public void getIntListVersionError() {
		DotVersion.getIntListVersion("1--224.0123");
	}

	@Test(expected = IllegalArgumentException.class)
	public void getIntListVersionError2() {
		DotVersion.getIntListVersion(".1-224.0123");
	}
	
	@Test
	public void testOrderLess() {
		DotVersion dv1 = DotVersion.getDotVersion("1.1.2");
		DotVersion dv2 = DotVersion.getDotVersion("1.2.1");
		Assert.assertTrue( dv1.compareTo(dv2) < 0 );
	}

	@Test
	public void testOrderMore() {
		DotVersion dv1 = DotVersion.getDotVersion("1.1.2");
		DotVersion dv2 = DotVersion.getDotVersion("1.1.1");
		Assert.assertTrue( dv1.compareTo(dv2) > 0 );
	}

	@Test
	public void testOrderEquals() {
		DotVersion dv1 = DotVersion.getDotVersion("1.1.01");
		DotVersion dv2 = DotVersion.getDotVersion("1.1.1");
		Assert.assertTrue( dv1.compareTo(dv2) == 0 );
	}
	
	@Test
	public void testOrderLess2() {
		DotVersion dv1 = DotVersion.getDotVersion("1.1.2");
		DotVersion dv2 = DotVersion.getDotVersion("1.1.2.1");
		Assert.assertTrue( dv1.compareTo(dv2) < 0 );
	}

	@Test
	public void testOrderMore2() {
		DotVersion dv1 = DotVersion.getDotVersion("1.2");
		DotVersion dv2 = DotVersion.getDotVersion("1.1.2");
		Assert.assertTrue( dv1.compareTo(dv2) > 0 );
	}
	
}

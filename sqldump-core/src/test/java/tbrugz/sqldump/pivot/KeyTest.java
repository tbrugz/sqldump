package tbrugz.sqldump.pivot;

import org.junit.Assert;
import org.junit.Test;

import tbrugz.sqldump.resultset.pivot.Key;

public class KeyTest {

	@Test
	public void compareTo() {
		Key k1 = new Key(new String[]{"1","2","3"});
		Key k2 = new Key(new String[]{"1","2","3","4"});
		Key k3 = new Key(new String[]{"1","2","4"});
		
		Assert.assertEquals(0, k1.compareTo(k1));
		Assert.assertEquals(-1, k1.compareTo(k2));
		Assert.assertEquals(1, k2.compareTo(k1));
		Assert.assertEquals(-1, k1.compareTo(k3));
		Assert.assertEquals(1, k3.compareTo(k1));
		Assert.assertEquals(-1, k2.compareTo(k3));
		Assert.assertEquals(1, k3.compareTo(k2));
	}
	
}

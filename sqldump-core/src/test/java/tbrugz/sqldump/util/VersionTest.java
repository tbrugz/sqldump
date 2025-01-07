package tbrugz.sqldump.util;

import java.util.regex.Pattern;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public class VersionTest {

	Pattern pVersion = Pattern.compile("^\\d+");
	Pattern pSha1 = Pattern.compile("[0-9a-f]{5,40}");

	@Test
	public void testVersion() {
		//System.out.println("Version.getVersion() = "+Version.getVersion());
		Assert.assertNotNull(Version.getVersion());
		boolean m = pVersion.matcher(Version.getVersion()).find();
		Assert.assertTrue(m);
	}

	@Test
	@Ignore("hg not working")
	public void testBuildNumber() {
		//System.out.println("Version.getBuildNumber() = "+Version.getBuildNumber());
		Assert.assertNotNull(Version.getBuildNumber());
		boolean m = pSha1.matcher(Version.getBuildNumber()).find();
		Assert.assertTrue(m);
	}
}

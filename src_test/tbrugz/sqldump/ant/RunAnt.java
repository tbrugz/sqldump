package tbrugz.sqldump.ant;

import java.io.File;

import org.apache.tools.ant.Project;
import org.apache.tools.ant.ProjectHelper;

public class RunAnt {
	
	static void runAntDefault() {
		runAnt(new File("build.xml"), null);
	}

	public static void runAnt(File buildFile) {
		runAnt(buildFile, null);
	}
	
	/*
	 * see: http://stackoverflow.com/questions/6733684/run-ant-from-java
	 */
	public static void runAnt(File buildFile, String target) {
		Project p = new Project();
		p.setUserProperty("ant.file", buildFile.getAbsolutePath());
		p.init();
		ProjectHelper helper = ProjectHelper.getProjectHelper();
		p.addReference("ant.projectHelper", helper);
		helper.parse(p, buildFile);
		if(target==null) {
			target = p.getDefaultTarget(); 
		}
		p.executeTarget(target);
	}
	
}

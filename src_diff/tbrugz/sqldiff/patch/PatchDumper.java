package tbrugz.sqldiff.patch;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import difflib.Delta;
import difflib.DiffUtils;
import difflib.Patch;
import tbrugz.sqldiff.DiffDumper;
import tbrugz.sqldiff.model.Diff;
import tbrugz.sqldiff.model.SchemaDiff;
import tbrugz.sqldump.util.Utils;

/*
 * Patch (unified) diff dumper using java-diff-utils <https://code.google.com/p/java-diff-utils/>
 * 
 * other similar(?) project: https://code.google.com/p/google-diff-match-patch/
 * 
 * XXX?: use CategorizedOut with possible patterns (like outfilepattern): [schemaname], [objecttype], [objectname] (& [changetype]?) - maybe not the "spirit" of patch files...
 * XXX: include grabber info: source (grabber: JDBCGrabber/XML/JSON, JDBC url, JDBC user), date, user, sqldump-version - add properties to SchemaModel?
 * XXX: option to include context lines (lines of context around the lines that differ) - https://www.gnu.org/software/diffutils/manual/html_node/Context-Format.html#Context-Format
 * - option to specify number of lines of context (default: 3 lines?)
 * 
 * see also:
 * http://en.wikipedia.org/wiki/Diff_utility#Unified_format
 * https://www.gnu.org/software/diffutils/manual/html_node/Unified-Format.html
 */
public class PatchDumper implements DiffDumper {

	static final Log log = LogFactory.getLog(PatchDumper.class);

	@Override
	public String type() {
		return "patch";
	}

	@Override
	public void dumpDiff(Diff diff, Writer writer) throws IOException {
		try {
			if(diff instanceof SchemaDiff) {
				SchemaDiff sd = (SchemaDiff) diff;
				//XXX: write diff header
				for(Diff d: sd.getChildren()) {
					diffOne(d, writer);
				}
			}
			else {
				diffOne(diff, writer);
			}
			writer.flush();
		}
		catch(RuntimeException e) {
			log.warn("Error generating patch: "+e.getMessage(), e);
			throw e;
		}
	}
	
	public void diffOne(Diff diff, Writer writer) throws IOException {
		try {
		List<String> original = bigStringToLines(diff.getPreviousDefinition());
		List<String> revised = bigStringToLines(diff.getDefinition());
		Patch patch = DiffUtils.diff(original, revised);
		
		//writer.write("### "+diff.getObjectType()+": "+diff.getNamedObject().getSchemaName()+"."+diff.getNamedObject().getName()+"\n");
		writeHeader(writer, diff);
		for (Delta delta : patch.getDeltas()) {
			//writer.write(delta.toString()+"\n");
			//System.out.println(delta);
			writer.write("@@ -"+delta.getOriginal().getPosition()+","+delta.getOriginal().size()
					+" +"+delta.getRevised().getPosition()+","+delta.getRevised().size()
					+" @@\n");
			writeLines(writer, delta.getOriginal().getLines(), "-");
			writeLines(writer, delta.getRevised().getLines(), "+");
		}
		//writer.write("###\n");
		}
		catch(RuntimeException e) {
			log.warn("Error diffing "+diff.getObjectType()+" '"+diff.getNamedObject().getSchemaName()+"."+diff.getNamedObject().getName()+"' ("+diff.getChangeType()+"): "+e);
			throw e;
		}
	}
	
	void writeHeader(Writer w, Diff diff) throws IOException {
		String objectId = diff.getObjectType()+": "+diff.getNamedObject().getSchemaName()+"."+diff.getNamedObject().getName();
		StringBuilder sb = new StringBuilder();
		for(int i=objectId.length();i<60;i++) { sb.append(" "); }
		
		w.write("diff "
			+objectId
			+sb.toString()
			+" # type: "+diff.getChangeType()+"\n");
	}
	
	void writeLines(Writer w, List<?> lines, String prepend) throws IOException {
		if(lines.size()==1 && lines.get(0).equals("")) { return; }
		for(Object s: lines) {
			w.write(prepend+s+"\n");
		}
	}

	@Override
	public void dumpDiff(Diff diff, File file) throws IOException {
		Utils.prepareDir(file);
		FileWriter fw = new FileWriter(file);
		dumpDiff(diff, fw);
		log.info(type()+" diff written to: "+file.getAbsolutePath());
		fw.close();
	}
	
	List<String> bigStringToLines(String s) {
		return Arrays.asList(s.split("\n"));
	}
	
}

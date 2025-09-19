package tbrugz.sqldiff.patch;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.github.difflib.DiffUtils;
import com.github.difflib.patch.AbstractDelta;
import com.github.difflib.patch.Patch;

import tbrugz.sqldiff.DiffDumper;
import tbrugz.sqldiff.SQLDiff;
import tbrugz.sqldiff.model.Diff;
import tbrugz.sqldiff.model.SchemaDiff;
import tbrugz.sqldump.util.StringUtils;
import tbrugz.sqldump.util.Utils;

/*
 * Patch (unified) diff dumper using java-diff-utils <https://code.google.com/p/java-diff-utils/>
 * 
 * other similar(?) project: https://code.google.com/p/google-diff-match-patch/
 * 
 * TODO: use CategorizedOut with possible patterns (like outfilepattern): [schemaname], [objecttype], [objectname] (& [changetype]?) - maybe not the "spirit" of patch files...
 * XXX: include grabber info: source (grabber: JDBCGrabber/XML/JSON, JDBC url, JDBC user), date, user, sqldump-version - add properties to SchemaModel?
 * XXXdone: option to include context lines (lines of context around the lines that differ) - https://www.gnu.org/software/diffutils/manual/html_node/Context-Format.html#Context-Format
 * - XXXdone option to specify number of lines of context (default: 3 lines?)
 * TODO: join hunk's contexts if line difference is smaller than (contextsize*2)
 * 
 * see also:
 * http://en.wikipedia.org/wiki/Diff_utility#Unified_format
 * https://www.gnu.org/software/diffutils/manual/html_node/Unified-Format.html
 */
public class PatchDumper implements DiffDumper {

	static final Log log = LogFactory.getLog(PatchDumper.class);
	
	public static final String PROP_PATCHFILE_CONTEXT = SQLDiff.PREFIX_PATCH+".contextsize";
	
	int context = 3;

	@Override
	public String type() {
		return "patch";
	}
	
	@Override
	public void setProperties(Properties prop) {
		context = Utils.getPropInt(prop, PROP_PATCHFILE_CONTEXT, context);
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
			String message = "Error generating patch: "+e;
			log.warn(message, e);
			writer.write("\n# "+message);
			writer.flush();
			throw e;
		}
	}
	
	/*
	https://github.com/java-diff-utils/java-diff-utils/commit/33f2d744abcb1898ba3b0538cbb5e499fbb0a236 -  Jun 28, 2018
	original -> source
	revised -> target
	*/
	public void diffOne(Diff diff, Writer writer) throws IOException {
		try {
		List<String> original = bigStringToLines(diff.getPreviousDefinition());
		List<String> revised = bigStringToLines(diff.getDefinition());
		Patch<String> patch = DiffUtils.diff(original, revised);
		
		//writer.write("### "+diff.getObjectType()+": "+diff.getNamedObject().getSchemaName()+"."+diff.getNamedObject().getName()+"\n");
		writeHeader(writer, diff);
		for (AbstractDelta<String> delta : patch.getDeltas()) {
			//writer.write(delta.toString()+"\n");
			//System.out.println(delta);
			int beforeDeltaContextSize = (delta.getSource().getPosition())>context?context:delta.getSource().getPosition();
			int beforeDeltaStart = delta.getSource().getPosition()-beforeDeltaContextSize;
			int beforeDeltaEnd = delta.getSource().getPosition()-1;
			
			int afterDeltaContextSize = ( delta.getSource().getPosition()+delta.getSource().size()+context < original.size()) ?
					context:(original.size()-delta.getSource().getPosition()-delta.getSource().size());
			int afterDeltaStart = delta.getSource().getPosition()+delta.getSource().size();
			int afterDeltaEnd = afterDeltaStart+afterDeltaContextSize-1;
			
			writer.write("@@ -"+(delta.getSource().getPosition()-beforeDeltaContextSize+1)+","+delta.getSource().size()
					+" +"+(delta.getTarget().getPosition()-beforeDeltaContextSize+1)+","+delta.getTarget().size()
					+" @@"
					/*+" DELTA:: o:"+delta.getSource().getPosition()+" r:"+delta.getTarget().getPosition()
					+" // BEFORE:: c:"+beforeDeltaContextSize+" | s:"+beforeDeltaStart+" | e:"+beforeDeltaEnd
					+" // AFTER:: c:"+afterDeltaContextSize+" | s:"+afterDeltaStart+" | e:"+afterDeltaEnd
					+" // ORIGINAL: "+original.size()+" | REVISED: "+revised.size()*/
					+"\n");
			writeLines(writer, original, beforeDeltaStart, beforeDeltaEnd, " ");
			writeLines(writer, delta.getSource().getLines(), "-");
			writeLines(writer, delta.getTarget().getLines(), "+");
			writeLines(writer, original, afterDeltaStart, afterDeltaEnd, " ");
		}
		//writer.write("###\n");
		}
		catch(RuntimeException e) {
			log.warn("Error diffing "+diff.getObjectType()+" '"+diff.getNamedObject().getQualifiedName()+"' ("+diff.getChangeType()+"): "+e);
			throw e;
		}
	}
	
	void writeHeader(Writer w, Diff diff) throws IOException {
		String objectId = diff.getObjectType()+": "+diff.getNamedObject().getQualifiedName();
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

	void writeLines(Writer w, List<?> lines, int start, int end, String prepend) throws IOException {
		if(lines.size()==1 && lines.get(0).equals("")) { return; }
		for(int i=start;i<=end;i++) {
			Object s = lines.get(i);
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
	
	//static final Pattern PTRN_TRAILING_WHITESPACE = Pattern.compile("\\s+$", Pattern.MULTILINE);
	
	//XXX: option to ignore or not trailing whitespace? - see equals4Diff...
	List<String> bigStringToLines(String s) {
		if(s==null) { s = ""; }
		s = StringUtils.PTRN_TRAILING_WHITESPACE.matcher(s).replaceAll("");
		return Arrays.asList(s.split("\r?\n"));
	}
	
}

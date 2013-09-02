package tbrugz.sqldump.sqlrun.importers;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import tbrugz.sqldump.util.Utils;

public class FFCImporter extends AbstractImporter {

	static final String SUFFIX_COLUMNSIZES = ".columnsizes";
	
	static final String[] FFC_AUX_SUFFIXES = {
		SUFFIX_COLUMNSIZES
	};
	
	int[] columnSizes;
	
	@Override
	void setImporterProperties(Properties prop, String importerPrefix) {
		super.setImporterProperties(prop, importerPrefix);
		List<String> colSizesStr = Utils.getStringListFromProp(prop, importerPrefix+SUFFIX_COLUMNSIZES, ",");
		columnSizes = new int[colSizesStr.size()];
		for(int i=0;i<colSizesStr.size();i++) {
			columnSizes[i] = Integer.parseInt(colSizesStr.get(i));
		}
	}
	
	@Override
	public List<String> getAuxSuffixes() {
		List<String> ret = super.getAuxSuffixes();
		ret.addAll(Arrays.asList(FFC_AUX_SUFFIXES));
		return ret;
	}
	
	@Override
	String[] procLine(String line, long processedLines) throws SQLException {
		String[] parts = new String[columnSizes.length];
		int startPos = 0, endPos = 0;
		for(int i=0;i<columnSizes.length;i++) {
			endPos = startPos+columnSizes[i];
			parts[i] = line.substring(startPos, endPos);
			startPos = endPos;
		}
		return parts;
	}

}

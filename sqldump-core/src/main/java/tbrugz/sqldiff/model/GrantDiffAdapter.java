package tbrugz.sqldiff.model;

import javax.xml.bind.annotation.adapters.XmlAdapter;

import tbrugz.sqldiff.model.adapted.GrntDiff;

public class GrantDiffAdapter extends XmlAdapter<GrntDiff, GrantDiff> {
	
	@Override
	public GrantDiff unmarshal(GrntDiff v) throws Exception {
		return new GrantDiff(v.schemaName, v.tableName, v.privilege, v.grantee, v.withGrantOption, v.changeType.equals(ChangeType.DROP));
	}

	@Override
	public GrntDiff marshal(GrantDiff v) throws Exception {
		GrntDiff adapted = new GrntDiff();
		adapted.changeType = v.getChangeType();
		adapted.schemaName = v.schemaName;
		adapted.tableName = v.tableName;
		adapted.privilege = v.privilege;
		adapted.grantee = v.grantee;
		adapted.withGrantOption = v.withGrantOption;
		return adapted;
	}

}

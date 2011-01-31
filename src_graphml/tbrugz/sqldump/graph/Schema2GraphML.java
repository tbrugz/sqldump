package tbrugz.sqldump.graph;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import tbrugz.graphml.model.Root;
import tbrugz.graphml.model.Link;
import tbrugz.graphml.model.Node;
import tbrugz.sqldump.FK;
import tbrugz.sqldump.SchemaModel;
import tbrugz.sqldump.Table;

public class Schema2GraphML {
	
	public static Root getGraphMlModel(SchemaModel schemaModel) {
		Root graphModel = new Root();
		
		Set<FK> fks = schemaModel.getForeignKeys();
		Map<String, Set<FK>> fkMap = new HashMap<String, Set<FK>>(); 
		for(FK fk: fks) {
			String fkId = fk.getSourceId();
			Set<FK> fkSet = fkMap.get(fkId);
			if(fkSet==null) {
				fkSet = new HashSet<FK>();
				fkMap.put(fkId, fkSet);
			}
			fkSet.add(fk);
		}
		
		for(Table t: schemaModel.getTables()) {
			Node n = new Node();
			String id = t.getSchemaName()+"."+t.getName();
			n.setId(id);
			n.setLabel(id); //XXX
			graphModel.elements.add(n);
			Set<FK> fkSet = fkMap.get(id);
			if(fkSet!=null) {
    			for(FK fk: fkSet) {
    				Link l = fkToLink(fk);
    				n.getProx().add(l);
    			}
			}
		}
		
		return graphModel;
	}
	
	static Link fkToLink(FK fk) {
		Link l = new Link();
		l.setOrigem(fk.getSourceId());
		l.setsDestino(fk.getTargetId());
		return l;
	}
}

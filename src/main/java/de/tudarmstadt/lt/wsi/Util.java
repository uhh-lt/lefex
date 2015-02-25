package de.tudarmstadt.lt.wsi;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.uima.jcas.JCas;

import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token;
import de.tudarmstadt.ukp.dkpro.core.api.syntax.type.dependency.Dependency;

public class Util {
	public static Collection<Dependency> collapseDependencies(JCas jCas, Collection<Dependency> deps, Collection<Token> tokens) {
		List<Dependency> collapsedDeps = new ArrayList<>(deps);
		for (Token token : tokens) {
			if (token.getPos() != null && token.getPos().getPosValue().equals("IN")) {
				List<Dependency> toRemove = new ArrayList<>();
				String depType = "prep_" + token.getCoveredText().toLowerCase();
				Token source = null;
				Token target = null;
				int begin = -1;
				int end = -1;
				for (Dependency dep : collapsedDeps) {
					if (dep.getGovernor() == token && dep.getDependencyType().toLowerCase().equals("mwe")) {
						depType = "prep_" + dep.getDependent().getCoveredText() + "_" + token.getCoveredText().toLowerCase();
						toRemove.add(dep);
					} else if (dep.getGovernor() == token && dep.getDependencyType().toLowerCase().equals("pobj")) {
						end = dep.getEnd();
						target = dep.getDependent();
						toRemove.add(dep);
					} else if (dep.getDependent() == token && dep.getDependencyType().toLowerCase().equals("prep")) {
						begin = dep.getBegin();
						source = dep.getGovernor();
						toRemove.add(dep);
					}
				}
				if (source != null && target != null) {
					Dependency collapsedDep = new Dependency(jCas, begin, end);
					collapsedDep.setGovernor(source);
					collapsedDep.setDependent(target);
					collapsedDep.setDependencyType(depType);
					collapsedDeps.add(collapsedDep);
					collapsedDeps.removeAll(toRemove);
				}
			}
		}
		return collapsedDeps;
	}
	
	public static String semantifyDependencyRelation(String rel) {
		switch (rel) {
		case "nsubj":
			return "subj";
		case "nsubjpass":
		case "partmod":
		case "infmod":
		case "vmod":
		case "dobj":
			return "obj";
		}
		return null;
	}
}

package de.tudarmstadt.lt.wsi.Utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.uima.jcas.JCas;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token;
import de.tudarmstadt.ukp.dkpro.core.api.syntax.type.dependency.Dependency;
import org.apache.uima.jcas.tcas.Annotation;


public class Format {

    public static String NOT_SEP = ";";

	private static String trimScore(String score){
		String[] parts = score.split("[:#]");
		return parts[0];
	}

	public static String flatten(List<String> list, int maxLength, boolean keepScores, String sep) {
		StringBuilder sb = new StringBuilder();
		int i = 0;
		for (String str : list) {
			if(i > maxLength) break;

			str = str.replace(",", NOT_SEP);
			str = str.replace(sep, NOT_SEP);
			if (keepScores) sb.append(str);
			else sb.append(trimScore(str));
			sb.append(sep);
			i++;
		}
		return sb.toString().trim();
	}

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
		return rel;
	}

    public static String join(List<String> list, String sep) {
        StringBuilder sb = new StringBuilder();
        int i = 0;
        for (String str: list) {
            i++;
            if (!str.equals("") && !str.contains(sep)) {
                sb.append(str);
                if (i < list.size()) sb.append(sep);
            }
        }
        return sb.toString().trim();
    }

    public static void printCasAnnotations(JCas jCas) {
        System.out.println("All annotations of the JCas");
        for (Annotation a : jCas.getAnnotationIndex()) {
            System.out.println(a.getType().toString());
        }
    }

}

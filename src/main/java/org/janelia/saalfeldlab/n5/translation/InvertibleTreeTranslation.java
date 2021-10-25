package org.janelia.saalfeldlab.n5.translation;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.janelia.saalfeldlab.n5.container.ContainerMetadataNode;

import com.google.gson.Gson;
import com.google.gson.JsonElement;

public class InvertibleTreeTranslation extends TreeTranslation {

	protected ContainerTranslation invFun;

	public InvertibleTreeTranslation( 
			final ContainerMetadataNode root,
			final Gson gson,
			final String fwd, final String inv) {
		super( root, gson, fwd );
		invFun = new JqContainerTranslation( inv, gson );
	}
	
	public ContainerTranslation getInverseTranslationFunction() {
		return invFun;
	}
	
	public void updateOriginal() {
		rootOrig = invFun.apply(rootTranslated);
		rootOrig.addPathsRecursive( );
	}

	public <T> void setTranslatedAttribute(String pathName, String key, T attribute) {
		Optional<ContainerMetadataNode> childOpt = rootTranslated.getNode(pathName);
		if (childOpt.isPresent()) {
			childOpt.get().getAttributes().put(key, gson.toJsonTree(attribute));
			updateOriginal();
		}
	}
	
	public <T> void setTranslatedAttributes(String pathName, Map<String, ?> attributes) {
		Optional<ContainerMetadataNode> childOpt = rootTranslated.getNode(pathName);
		if (childOpt.isPresent()) {

			HashMap<String, JsonElement> destAttrs = childOpt.get().getAttributes();
			for( String k : attributes.keySet() )
				destAttrs.put(k, gson.toJsonTree(attributes.get(k)));

			updateOriginal();
		}	
	}

}

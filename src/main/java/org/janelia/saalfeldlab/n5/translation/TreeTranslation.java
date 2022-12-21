package org.janelia.saalfeldlab.n5.translation;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.janelia.saalfeldlab.n5.container.ContainerMetadataNode;

import com.google.gson.Gson;
import com.google.gson.JsonElement;

public class TreeTranslation {

	protected ContainerMetadataNode rootOrig;
	protected ContainerMetadataNode rootTranslated;
	protected JqContainerTranslation fwdFun;
	protected Gson gson;

	public TreeTranslation( 
			final ContainerMetadataNode root,
			final Gson gson,
			final String fwd ) {
		this.rootOrig = root;
		this.gson = gson;
		fwdFun = new JqContainerTranslation( fwd, gson );

		updateTranslated();
	}

	public JqContainerTranslation getTranslationFunction() {
		return fwdFun;
	}

	public ContainerMetadataNode getOrig() {
		return rootOrig;
	}

	public ContainerMetadataNode getTranslated() {
		return rootTranslated;
	}
	
	public void updateTranslated() {
		rootTranslated = fwdFun.apply(rootOrig);
		rootTranslated.addPathsRecursive();
	}

	public <T> void setAttribute( String pathName, String key, T attribute ) {
		rootOrig.setAttribute(pathName, key, attribute);
		updateTranslated();
	}
	
	public <T> void setAttributes( String pathName, Map<String,?> attributes ) { 
		rootOrig.setAttributes(pathName, attributes);
		updateTranslated();
	}

}

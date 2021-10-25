package org.janelia.saalfeldlab.n5.translation;

import org.janelia.saalfeldlab.n5.container.ContainerMetadataNode;

import com.google.gson.Gson;

public class JqContainerTranslation extends JqFunction<ContainerMetadataNode,ContainerMetadataNode> 
	implements ContainerTranslation {

	public JqContainerTranslation(String translation, Gson gson ) {
		super(translation, gson, ContainerMetadataNode.class );
	}

}

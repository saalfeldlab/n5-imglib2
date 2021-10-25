package org.janelia.saalfeldlab.n5.translation;

import java.util.function.Function;

import org.janelia.saalfeldlab.n5.container.ContainerMetadataNode;

public interface ContainerTranslation extends Function<ContainerMetadataNode,ContainerMetadataNode> {
	
}


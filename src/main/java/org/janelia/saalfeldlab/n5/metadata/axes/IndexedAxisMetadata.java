package org.janelia.saalfeldlab.n5.metadata.axes;

import java.util.stream.IntStream;

/**
 * Metadata that labels and assigns types to axes. 
 *
 * @author John Bogovic
 *
 */
public interface IndexedAxisMetadata extends AxisMetadata {

	public default int[] getIndexes() {
		return IntStream.range(0, getAxisTypes().length ).toArray();
	}

}

package org.janelia.saalfeldlab.n5.metadata.omengff;

import org.janelia.saalfeldlab.n5.N5Reader;

import net.imglib2.realtransform.InvertibleRealTransform;

/**
 * An interface for invertible transformations.
 * 
 * @author John Bogovic
 *
 * @param <T>
 */
public interface NgffInvertibleCoordinateTransformation< T extends InvertibleRealTransform > extends NgffCoordinateTransformation< T >
{
	public default boolean isInvertible() {
		return true;
	}
	
	/**
	 * Returns an {@link InvertibleRealTransform}.
	 *
	 * @param n5 the n5 reader
	 * @return the invertible transform
	 */
	public default InvertibleRealTransform getInvertibleTransform( N5Reader n5 )
	{
		return getTransform(  n5 );
	}

}

package org.janelia.saalfeldlab.n5.metadata.omengff;

import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.metadata.transforms.SpatialTransform;

import net.imglib2.realtransform.InvertibleRealTransform;
import net.imglib2.realtransform.RealTransform;
import ome.ngff.transformations.AffineTransformation;
import ome.ngff.transformations.BijectionTransformation;
import ome.ngff.transformations.CoordinateTransformation;
import ome.ngff.transformations.DisplacementsTransformation;
import ome.ngff.transformations.IdentityTransformation;
import ome.ngff.transformations.ScaleTransformation;
import ome.ngff.transformations.SequenceTransformation;
import ome.ngff.transformations.TranslationTransformation;

public interface NgffCoordinateTransformation<T extends RealTransform> extends SpatialTransform, CoordinateTransformation
{
	public static NgffCoordinateTransformation<?> create( CoordinateTransformation ct )
	{
		if( ct instanceof NgffCoordinateTransformation )
		{
			return (NgffCoordinateTransformation)ct;
		}

		switch( ct.getType() )
		{
			case IdentityTransformation.TYPE:
				return new NgffIdentityTransformation( (IdentityTransformation)ct );
			case ScaleTransformation.TYPE:
				return new NgffScaleTransformation( (ScaleTransformation)ct );
			case TranslationTransformation.TYPE:
				return new NgffTranslationTransformation( (TranslationTransformation)ct );
			case AffineTransformation.TYPE:
				return new NgffAffineTransformation( (AffineTransformation)ct );
			case SequenceTransformation.TYPE:
				return new NgffSequenceTransformation( (SequenceTransformation)ct );
			case DisplacementsTransformation.TYPE:
				return new NgffDisplacementsTransformation( (DisplacementsTransformation)ct );
			case BijectionTransformation.TYPE:
				return new NgffBijectionTransformation( (BijectionTransformation)ct );
		
		}
		return null;
	}

	public T getTransform( N5Reader n5 );

	public default boolean isInvertible() {
		return false;
	}

	/**
	 * Returns an {@link InvertibleRealTransform} if the transformation is invertible,
	 * otherwise, returns null.
	 *
	 * @param n5 the n5 reader
	 * @return the invertible transform or null
	 */
	public default InvertibleRealTransform getInvertibleTransform( N5Reader n5 )
	{
		return null;
	}

}

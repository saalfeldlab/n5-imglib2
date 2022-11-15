package org.janelia.saalfeldlab.n5.metadata.omengff;

import org.janelia.saalfeldlab.n5.N5Reader;

import net.imglib2.realtransform.InvertibleRealTransformSequence;
import ome.ngff.transformations.IdentityTransformation;

public class NgffIdentityTransformation extends IdentityTransformation implements NgffInvertibleCoordinateTransformation< InvertibleRealTransformSequence >
{
	public NgffIdentityTransformation( IdentityTransformation other )
	{
		super( other );
	}

	public NgffIdentityTransformation( String name, String input, String output )
	{
		super( name, input, output );
	}

	@Override
	public InvertibleRealTransformSequence getTransform()
	{
		// an empty sequence is the identity
		return new InvertibleRealTransformSequence();
	}

	@Override
	public InvertibleRealTransformSequence getTransform( N5Reader n5 )
	{
		return getTransform();
	}

}

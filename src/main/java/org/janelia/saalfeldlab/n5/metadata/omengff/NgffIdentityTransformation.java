package org.janelia.saalfeldlab.n5.metadata.omengff;

import net.imglib2.realtransform.RealTransform;
import net.imglib2.realtransform.RealTransformSequence;
import ome.ngff.transformations.IdentityTransformation;

public class NgffIdentityTransformation extends IdentityTransformation implements NgffCoordinateTransformation< RealTransform >
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
	public RealTransform getTransform()
	{
		// an empty sequence is the identity
		return new RealTransformSequence();
	}

}

package org.janelia.saalfeldlab.n5.metadata.omengff;

import org.janelia.saalfeldlab.n5.N5Reader;

import net.imglib2.realtransform.ExplicitInvertibleRealTransform;
import ome.ngff.transformations.BijectionTransformation;
import ome.ngff.transformations.CoordinateTransformation;

public class NgffBijectionTransformation extends BijectionTransformation
	implements NgffInvertibleCoordinateTransformation< ExplicitInvertibleRealTransform >
{

	public NgffBijectionTransformation( final String name, final String input, final String output,
			final CoordinateTransformation forward, final CoordinateTransformation inverse ) {
		super( name, input, output, forward, inverse );
	}

	public NgffBijectionTransformation( final String input, final String output, 
			final CoordinateTransformation forward, final CoordinateTransformation inverse ) {
		this( null, input, output, forward, inverse );
	}

	public NgffBijectionTransformation( BijectionTransformation other )
	{
		super( other );
	}

	@Override
	public ExplicitInvertibleRealTransform getTransform()
	{
		return new ExplicitInvertibleRealTransform( 
				NgffCoordinateTransformation.create( forward ).getTransform(),
				NgffCoordinateTransformation.create( inverse ).getTransform());
	}

	@Override
	public ExplicitInvertibleRealTransform getTransform( N5Reader n5 )
	{
		return new ExplicitInvertibleRealTransform( 
				NgffCoordinateTransformation.create( forward ).getTransform( n5 ),
				NgffCoordinateTransformation.create( inverse ).getTransform( n5 ));
	}

}
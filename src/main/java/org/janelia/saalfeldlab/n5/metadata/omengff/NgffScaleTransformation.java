package org.janelia.saalfeldlab.n5.metadata.omengff;

import net.imglib2.realtransform.Scale;
import net.imglib2.realtransform.Scale2D;
import net.imglib2.realtransform.Scale3D;
import net.imglib2.realtransform.ScaleGet;
import ome.ngff.transformations.ScaleTransformation;

public class NgffScaleTransformation extends ScaleTransformation
		implements NgffCoordinateTransformation<  ScaleGet >
{

	public NgffScaleTransformation( String input, String output, double[] scale )
	{
		super( input, output, scale );
	}
	
	public NgffScaleTransformation( ScaleTransformation other )
	{
		super( other );
	}

	@Override
	public ScaleGet getTransform()
	{
		int nd = getScale().length;
		if( nd == 2 )
			return new Scale2D( getScale() );
		else if( nd == 3 )
			return new Scale3D( getScale() );
		else
			return new Scale( getScale() );
	}

}

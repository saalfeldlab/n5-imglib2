package org.janelia.saalfeldlab.n5.metadata.omengff;

import org.janelia.saalfeldlab.n5.N5Reader;

import net.imglib2.realtransform.AffineGet;
import net.imglib2.realtransform.AffineTransform;
import net.imglib2.realtransform.AffineTransform2D;
import net.imglib2.realtransform.AffineTransform3D;
import ome.ngff.transformations.AffineTransformation;

public class NgffAffineTransformation extends AffineTransformation
		implements NgffInvertibleCoordinateTransformation< AffineGet >
{

	public NgffAffineTransformation( String input, String output, double[] affine )
	{
		super( input, output, affine );
	}

	public NgffAffineTransformation( double[] affine )
	{
		super( null, null, affine );
	}

	public NgffAffineTransformation( AffineTransformation other )
	{
		super( other );
	}

	@Override
	public AffineGet getTransform( N5Reader n5 )
	{
		return getTransform();
	}

	@Override
	public AffineGet getTransform()
	{
		// TODO allow num input != num output dimensions  
		int N = getAffine().length;

		int nd = -1;
		for( int i = 0; i < Math.ceil(Math.sqrt( N )) + 1; i++ )
		{
			if( N == i * ( i + 1 ))
			{
				nd = i;
				break;
			}
		}

		if( nd < 0 )
			return null;

		if( nd == 2 )
		{
			final AffineTransform2D out = new AffineTransform2D();
			out.set( getAffine() );
			return out;
		}
		else if( nd == 3 )
		{
			final AffineTransform3D out = new AffineTransform3D();
			out.set( getAffine() );
			return out;
		}
		else
		{
			final AffineTransform out = new AffineTransform( nd );
			out.set( getAffine() );
			return out;
		}
	}

}

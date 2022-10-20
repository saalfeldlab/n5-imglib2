package org.janelia.saalfeldlab.n5.metadata.omengff;

import java.util.Arrays;

import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.metadata.transforms.RealCoordinateTransform;

import net.imglib2.realtransform.AffineGet;
import net.imglib2.realtransform.AffineTransform;
import net.imglib2.realtransform.RealTransformSequence;
import ome.ngff.transformations.CoordinateTransformation;
import ome.ngff.transformations.SequenceTransformation;

public class NgffSequenceTransformation extends SequenceTransformation
	implements NgffCoordinateTransformation< RealTransformSequence >
//	implements RealCoordinateTransform<RealTransformSequence> 
{

	public NgffSequenceTransformation( final String name, final String input, final String output, final CoordinateTransformation[] transformations) {
		super( name, input, output, transformations );
	}

	public NgffSequenceTransformation( final String input, final String output, final CoordinateTransformation[] transformations) {
		this( null, input, output, transformations );
	}

	public NgffSequenceTransformation( SequenceTransformation other )
	{
		super( other );
	}

	@Override
	public RealTransformSequence getTransform()
	{
		final RealTransformSequence seq = new RealTransformSequence();
		Arrays.stream( transformations )
			.map( NgffCoordinateTransformation::create )
			.forEach( x -> seq.add( x.getTransform() ));
		return seq;
	}

	@Override
	public RealTransformSequence getTransform( N5Reader n5 )
	{
		final RealTransformSequence seq = new RealTransformSequence();
		Arrays.stream( transformations )
			.map( NgffCoordinateTransformation::create )
			.forEach( x -> seq.add( x.getTransform( n5 ) ));
		return seq;
	}

	public boolean isAffine()
	{
		return Arrays.stream( transformations )
			.map( NgffCoordinateTransformation::create )
			.allMatch( x -> x.getTransform() instanceof AffineGet );
	}

	public AffineGet asAffine( int nd )
	{
		AffineTransform affine = new AffineTransform( nd );
		for( CoordinateTransformation t : transformations )
		{
			NgffCoordinateTransformation< ? > nt = NgffCoordinateTransformation.create( t );
//			AffineGet tform  (AffineGet)nt.getTransform();=
			affine.preConcatenate(  (AffineGet)nt.getTransform() );
		}
//		Arrays.stream( transformations )
//			.map( NgffCoordinateTransformation::create )
//			.forEach( x -> affine.preConcatenate( ( AffineGet ) x.getTransform() ));

		return affine;
	}
	
}
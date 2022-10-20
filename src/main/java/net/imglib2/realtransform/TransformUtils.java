package net.imglib2.realtransform;

import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;

import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.metadata.omengff.NgffCoordinateTransformation;
import org.janelia.saalfeldlab.n5.metadata.omengff.NgffIdentityTransformation;
import org.janelia.saalfeldlab.n5.metadata.omengff.NgffScaleTransformation;
import org.janelia.saalfeldlab.n5.metadata.omengff.NgffSequenceTransformation;
import org.janelia.saalfeldlab.n5.metadata.omengff.NgffTranslationTransformation;

import net.imglib2.RandomAccessible;
import net.imglib2.interpolation.InterpolatorFactory;
import net.imglib2.interpolation.randomaccess.NLinearInterpolatorFactory;
import net.imglib2.interpolation.randomaccess.NearestNeighborInterpolatorFactory;
import net.imglib2.type.numeric.NumericType;
import ome.ngff.transformations.ParametrizedInterpolatedCoordinateTransformation;

public class TransformUtils
{
	
	public static AffineGet subAffine( AffineGet affine, int[] indexes  )
	{
		final int nd = indexes.length;
		if( nd == 2 )
		{
			AffineTransform2D out = new AffineTransform2D();
			out.set( rowPackedSubMatrix( affine, indexes ) );
			return out;
		}
		else if( nd == 3 )
		{
			AffineTransform3D out = new AffineTransform3D();
			out.set( rowPackedSubMatrix( affine, indexes ) );
			return out;
			
		}
		else
		{
			AffineTransform out = new AffineTransform( nd );
			out.set( rowPackedSubMatrix( affine, indexes ) );
			return out;
		}
	}

	private static double[] rowPackedSubMatrix( AffineGet affine, int[] indexes )
	{
		final int ndIn = affine.numTargetDimensions();
		final int nd = indexes.length; 
		double[] dat = new double[ nd * ( nd + 1 ) ];
		int k = 0;
		for( int i = 0; i < nd; i++ ) {
			for( int j = 0; j < nd; j++ ) {
				dat[k++] = affine.get( indexes[i], indexes[j] );
			}
			dat[k++] = affine.get( indexes[i], ndIn );
		}
		return dat;
	}

	public static AffineTransform3D toAffine3D( NgffSequenceTransformation seq )
	{
		return toAffine3D( 
			Arrays.stream( seq.getTransformations() )
				.map( x -> { return NgffCoordinateTransformation.create( x ); 	} )
				.collect( Collectors.toList() ));
	}

	public static AffineTransform3D toAffine3D( Collection<NgffCoordinateTransformation<?>> transforms )
	{
		return toAffine3D( null, transforms );
	}
	
	public static AffineGet toAffine( NgffCoordinateTransformation< ? > transform, int nd )
	{
		if( transform.getType().equals( NgffScaleTransformation.TYPE ))
		{
			return ((NgffScaleTransformation)transform).getTransform();
		}
		else if( transform.getType().equals( NgffTranslationTransformation.TYPE ))
		{
			return ((NgffTranslationTransformation)transform).getTransform();
		}
		else if( transform.getType().equals( NgffSequenceTransformation.TYPE ))
		{	
			NgffSequenceTransformation seq = (NgffSequenceTransformation) transform;
			if( seq.isAffine() )
				return seq.asAffine( nd );
			else
				return null;
		}
		else
			return null;
	}

	public static AffineTransform3D toAffine3D( N5Reader n5, Collection<NgffCoordinateTransformation<?>> transforms )
	{
		final AffineTransform3D total = new AffineTransform3D();
		for( NgffCoordinateTransformation<?> ct : transforms )
		{
			if( ct instanceof NgffIdentityTransformation )
				continue;
			else if( ct instanceof NgffSequenceTransformation )
			{
				AffineTransform3D t = toAffine3D( (NgffSequenceTransformation)ct );
				if( t == null )
					return null;
				else
					preConcatenate( total, (AffineGet) t  );
			}
			else {
				Object t = ct.getTransform(n5);
				if( t instanceof AffineGet )
				{
					preConcatenate( total, (AffineGet) t  );
	//				total.preConcatenate((AffineGet) t );
				}
				else
					return null;
			}
		}
		return total;
	}
	
	public static void preConcatenate( AffineTransform3D tgt, AffineGet concatenate )
	{
		if( concatenate.numTargetDimensions() >= 3 )
			tgt.preConcatenate(concatenate);
		else if( concatenate.numTargetDimensions() == 2 )
		{
			AffineTransform3D c = new AffineTransform3D();
			c.set(
					concatenate.get(0, 0), concatenate.get(0, 1), 0, concatenate.get(0, 2),
					concatenate.get(1, 0), concatenate.get(1, 1), 0, concatenate.get(1, 2),
					0, 0, 1, 0);

			tgt.preConcatenate(c);
		}
		else if( concatenate.numTargetDimensions() == 1 )
		{
			ScaleAndTranslation c = new ScaleAndTranslation(
					new double[]{ 1, 1, 1 },
					new double[]{ 0, 0, 0});
			tgt.preConcatenate(c);
		}
	}
	
	public static < T extends NumericType< T > > InterpolatorFactory< T, RandomAccessible< T > > interpolator( String interpolator )
	{
		switch( interpolator )
		{
		case ParametrizedInterpolatedCoordinateTransformation.LINEAR_INTERPOLATION:
			return new NLinearInterpolatorFactory<T>();
		case ParametrizedInterpolatedCoordinateTransformation.NEAREST_INTERPOLATION:
			return new NearestNeighborInterpolatorFactory<>();
		default:
			return null;
		}
	}

}

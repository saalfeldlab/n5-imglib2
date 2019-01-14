package org.janelia.saalfeldlab.n5.imglib2;

import java.io.IOException;

import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5Writer;

import net.imglib2.Cursor;
import net.imglib2.FinalInterval;
import net.imglib2.Interval;
import net.imglib2.IterableInterval;
import net.imglib2.RandomAccessible;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.RealRandomAccessible;
import net.imglib2.converter.Converter;
import net.imglib2.converter.Converters;
import net.imglib2.interpolation.InterpolatorFactory;
import net.imglib2.interpolation.randomaccess.NLinearInterpolatorFactory;
import net.imglib2.realtransform.AffineGet;
import net.imglib2.realtransform.AffineTransform;
import net.imglib2.realtransform.AffineTransform2D;
import net.imglib2.realtransform.AffineTransform3D;
import net.imglib2.realtransform.DeformationFieldTransform;
import net.imglib2.realtransform.InverseRealTransform;
import net.imglib2.realtransform.InvertibleRealTransform;
import net.imglib2.realtransform.InvertibleRealTransformSequence;
import net.imglib2.realtransform.RealTransform;
import net.imglib2.realtransform.RealTransformRandomAccessible;
import net.imglib2.realtransform.RealTransformSequence;
import net.imglib2.realtransform.RealViews;
import net.imglib2.transform.integer.MixedTransform;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.integer.ByteType;
import net.imglib2.type.numeric.integer.IntType;
import net.imglib2.type.numeric.integer.LongType;
import net.imglib2.type.numeric.integer.ShortType;
import net.imglib2.type.numeric.integer.UnsignedByteType;
import net.imglib2.type.numeric.integer.UnsignedIntType;
import net.imglib2.type.numeric.integer.UnsignedLongType;
import net.imglib2.type.numeric.integer.UnsignedShortType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.Util;
import net.imglib2.view.IntervalView;
import net.imglib2.view.MixedTransformView;
import net.imglib2.view.Views;

public class N5DisplacementField
{
	public static final String MULTIPLIER_ATTR = "quantization_multiplier";
	public static final String AFFINE_ATTR = "affine";
	public static final String SPACING_ATTR = "spacing";
//	public static final String FIELD_ATTR = "dfield";

	public static final <T extends NativeType<T> & RealType<T>> void save(
			final AffineGet affine,
			final RandomAccessibleInterval< T > dfield,
			final double[] spacing,
			final N5Writer n5Writer,
			final String dataset,
			final int[] blockSize,
			final Compression compression ) throws IOException
	{
		N5Utils.save( dfield, n5Writer, dataset, blockSize, compression );
		saveAffine( affine, n5Writer, dataset );
		if( spacing != null )
			n5Writer.setAttribute( dataset, SPACING_ATTR, spacing );
	}

	public static final <T extends NativeType<T> & RealType<T>, Q extends NativeType<Q> & RealType<Q>> void save(
			final AffineGet affine,
			final RandomAccessibleInterval< T > dfield,
			final double[] spacing,
			final N5Writer n5Writer,
			final String dataset,
			final int[] blockSize,
			final Compression compression, 
			final Q outputType, 
			final double maxError ) throws Exception
	{
		
		saveQuantized( dfield, n5Writer, dataset, blockSize, compression, outputType, maxError );
		saveAffine( affine, n5Writer, dataset );
		if( spacing != null )
			n5Writer.setAttribute( dataset, SPACING_ATTR, spacing );
	}

	public static final void saveAffine(
			final AffineGet affine,
			final N5Writer n5Writer,
			final String dataset ) throws IOException
	{
		if( affine != null )
			n5Writer.setAttribute( dataset, AFFINE_ATTR,  affine.getRowPackedCopy() );
	}

	public static final <T extends RealType<T>, Q extends NativeType<Q> & RealType<Q>> void saveQuantized(
			RandomAccessibleInterval<T> source,
			final N5Writer n5Writer,
			final String dataset,
			final int[] blockSize,
			final Compression compression,
			final Q outputType,
            final double maxError ) throws Exception
	{
		/* 
		 * To keep the max vector error below maxError, 
		 * the error per coordinate must be below m
		 */
		int nd = ( source.numDimensions() - 1 ); // vector field source has num dims + 1
		double m = 2 * Math.sqrt( maxError * maxError / nd );

		RandomAccessibleInterval< T > source_permuted = vectorAxisFirst( source );
		RandomAccessibleInterval< Q > source_quant = Converters.convert(
				source_permuted, 
				new Converter<T, Q>()
				{
					@Override
					public void convert(T input, Q output)
					{
						output.setReal( Math.round( input.getRealDouble() / m ));
					}
				}, 
				outputType.copy());

        N5Utils.save( source_quant, n5Writer, dataset, blockSize, compression);
		n5Writer.setAttribute( dataset, MULTIPLIER_ATTR, m );
	}
	
	public static final <T extends RealType<T> & NativeType<T>> RealTransform open( 
			final N5Reader n5,
			final String dataset,
			final boolean inverse,
			final T defaultType,
			final InterpolatorFactory< T, RandomAccessible<T> > interpolator ) throws Exception
	{

		AffineGet affine = openAffine( n5, dataset );

		DeformationFieldTransform< T > dfield = new DeformationFieldTransform<>(
				openCalibratedField( n5, dataset, interpolator, defaultType ));
		
		if( affine != null )
		{
			RealTransformSequence xfmSeq = new RealTransformSequence();
			if( inverse )
			{
				xfmSeq.add( affine );
				xfmSeq.add( dfield );
			}
			else
			{
				xfmSeq.add( dfield );
				xfmSeq.add( affine );
			}
			return xfmSeq;
		}
		else
		{
			return dfield;
		}
	}

	public static final AffineGet openPixelToPhysical( final N5Reader n5, final String dataset ) throws Exception
	{
		double[] spacing = n5.getAttribute( dataset, SPACING_ATTR, double[].class );
		if ( spacing == null )
			return null;

		// have to bump the dimension up by one to apply it to the displacement field
		int N = spacing.length;
		final AffineTransform affineMtx;
		if ( N == 1 )
			affineMtx = new AffineTransform( 2 );
		else if ( N == 2 )
			affineMtx = new AffineTransform( 3 );
		else if ( N == 3 )
			affineMtx = new AffineTransform( 4 );
		else
			return null;

		for ( int i = 0; i < N; i++ )
			affineMtx.set( spacing[ i ], i, i );

		return affineMtx;
	}

	public static final AffineGet openAffine( final N5Reader n5, final String dataset ) throws Exception
	{
		double[] affineMtxRow = n5.getAttribute( dataset, AFFINE_ATTR, double[].class );
		if ( affineMtxRow == null )
			return null;

		int N = affineMtxRow.length;
		final AffineTransform affineMtx;
		if ( N == 2 )
			affineMtx = new AffineTransform( 1 );
		else if ( N == 6 )
			affineMtx = new AffineTransform( 2 );
		else if ( N == 12 )
			affineMtx = new AffineTransform( 3 );
		else
			return null;

		affineMtx.set( affineMtxRow );
		return affineMtx;
	}

	@SuppressWarnings( "unchecked" )
	public static final <T extends NativeType<T> & RealType<T>, Q extends NativeType<Q> & RealType<Q>> RandomAccessibleInterval< T > openField( 
			final N5Reader n5,
			final String dataset,
			final T defaultType ) throws Exception
	{
		final DatasetAttributes attributes = n5.getDatasetAttributes(dataset);
		switch (attributes.getDataType()) {
		case INT8:
			return openQuantized( n5, dataset, (Q)new ByteType(), defaultType );
		case UINT8:
			return openQuantized( n5, dataset, (Q)new UnsignedByteType(), defaultType );
		case INT16:
			return openQuantized( n5, dataset, (Q)new ShortType(), defaultType );
		case UINT16:
			return openQuantized( n5, dataset, (Q)new UnsignedShortType(), defaultType );
		case INT32:
			return openQuantized( n5, dataset, (Q)new IntType(), defaultType );
		case UINT32:
			return openQuantized( n5, dataset, (Q)new UnsignedIntType(), defaultType );
		case INT64:
			return openQuantized( n5, dataset, (Q)new LongType(), defaultType );
		case UINT64:
			return openQuantized( n5, dataset, (Q)new UnsignedLongType(), defaultType );
		default:
			return openRaw( n5, dataset, defaultType );
		}
	}

	public static < T extends NativeType< T > & RealType< T > > RealRandomAccessible< T > openCalibratedField(
			final N5Reader n5, final String dataset,
			final InterpolatorFactory< T, RandomAccessible< T > > interpolator, 
			final T defaultType ) throws Exception
	{
		RandomAccessibleInterval< T > dfieldRai = openField( n5, dataset, defaultType );
		RandomAccessibleInterval< T > dfieldRaiPerm = vectorAxisLast( dfieldRai );

		if ( dfieldRai == null )
		{
			return null;
		}

		RealRandomAccessible< T > dfieldReal = Views.interpolate( Views.extendZero( dfieldRaiPerm ), interpolator );

		final AffineGet pix2Phys = openPixelToPhysical( n5, dataset );
		if ( pix2Phys != null )
			return RealViews.affine( dfieldReal, pix2Phys );

		return dfieldReal;
	}

	public static < T extends NativeType< T > & RealType< T > > RealTransform open(
			final N5Reader n5, final String dataset, boolean inverse ) throws Exception
	{
		return open( n5, dataset, inverse, new FloatType(), new NLinearInterpolatorFactory<FloatType>());
	}

	public static final < T extends RealType<T> & NativeType<T> > RandomAccessibleInterval< T > openRaw(
			final N5Reader n5,
			final String dataset,
			final T defaultType ) throws Exception
	{
        RandomAccessibleInterval< T > src = N5Utils.open( n5, dataset, defaultType  );
        return vectorAxisLast( src );
	}

	public static final <Q extends RealType<Q> & NativeType<Q>, T extends RealType<T>> RandomAccessibleInterval< T > openQuantized(
			final N5Reader n5,
			final String dataset,
			final Q defaultQuantizedType,
			final T defaultType ) throws Exception
	{
        RandomAccessibleInterval< Q > src = N5Utils.open( n5, dataset, defaultQuantizedType  );
        
        // get the factor going from quantized to original values
        Double mattr = n5.getAttribute( dataset, MULTIPLIER_ATTR, Double.TYPE );
        final double m;
        if( mattr != null )
        	m = mattr.doubleValue();
        else
        	m = 1.0;

        RandomAccessibleInterval< Q > src_perm = vectorAxisLast( src );
        RandomAccessibleInterval< T > src_converted = Converters.convert(
        	src_perm, 
			new Converter<Q, T>()
			{
				@Override
				public void convert(Q input, T output) {
					output.setReal( input.getRealDouble() * m );
				}
			}, 
			defaultType.copy());
        
        return src_converted;
	}

	public static final < T extends RealType< T > > RandomAccessibleInterval< T > vectorAxisLast( RandomAccessibleInterval< T > source ) throws Exception
	{
		final int n = source.numDimensions();

		if ( source.dimension( n - 1 ) == (n - 1) )
			return source;
		else if ( source.dimension( 0 ) == (n - 1) )
		{
			final int[] component = new int[ n ];
			component[ 0 ] = n - 1;
			for ( int i = 1; i < n; ++i )
				component[ i ] = i - 1;

			return permute( source, component );
		}

		throw new Exception( 
				String.format( "Displacement fields must store vector components in the first or last dimension. " + 
						"Found a %d-d volume; expect size [%d,...] or [...,%d]", n, ( n - 1 ), ( n - 1 ) ) );
	}
	
	public static final < T extends RealType< T > > RandomAccessibleInterval< T > vectorAxisFirst( RandomAccessibleInterval< T > source ) throws Exception
	{
		final int n = source.numDimensions();
		
		System.out.println("source2perm: " + Util.printInterval(source));

		if ( source.dimension( 0 ) == (n - 1) )
			return source;
		else if ( source.dimension( n - 1 ) == (n - 1) )
		{
			final int[] component = new int[ n ];
			component[ n - 1 ] = 0;
			for ( int i = 0; i < n-1; ++i )
				component[ i ] = i + 1;

			return permute( source, component );
		}

		throw new Exception( 
				String.format( "Displacement fields must store vector components in the first or last dimension. " + 
						"Found a %d-d volume; expect size [%d,...] or [...,%d]", n, ( n - 1 ), ( n - 1 ) ) );
	}

	public static final < T > IntervalView< T > permute( RandomAccessibleInterval< T > source, int[] p )
	{
		final int n = source.numDimensions();

		final long[] min = new long[ n ];
		final long[] max = new long[ n ];
		for ( int i = 0; i < n; ++i )
		{
			min[ p[ i ] ] = source.min( i );
			max[ p[ i ] ] = source.max( i );
		}

		final MixedTransform t = new MixedTransform( n, n );
		t.setComponentMapping( p );

		return Views.interval( new MixedTransformView< T >( source, t ), min, max );
	}

	public static <T extends RealType<T>> double[] getMinMax( IterableInterval<T> img )
	{
		double min = Double.MAX_VALUE;
		double max = Double.MIN_VALUE;
		Cursor<T> c = img.cursor();
		while( c.hasNext() )
		{
			double v = Math.abs( c.next().getRealDouble());
			if( v > max )
				max = v;
			
			if( v < min )
				min = v;
		}
		return new double[]{ min, max };
	}

}

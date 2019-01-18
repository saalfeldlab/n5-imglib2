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
import net.imglib2.realtransform.ExplicitInvertibleRealTransform;
import net.imglib2.realtransform.InverseRealTransform;
import net.imglib2.realtransform.InvertibleRealTransform;
import net.imglib2.realtransform.InvertibleRealTransformSequence;
import net.imglib2.realtransform.RealTransform;
import net.imglib2.realtransform.RealTransformRandomAccessible;
import net.imglib2.realtransform.RealTransformSequence;
import net.imglib2.realtransform.RealViews;
import net.imglib2.transform.integer.MixedTransform;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.IntegerType;
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

/**
 * Class with helper methods for saving displacement field transformations as N5 datasets.
 * 
 * @author John Bogovic
 *
 */
public class N5DisplacementField
{
	public static final String MULTIPLIER_ATTR = "quantization_multiplier";
	public static final String AFFINE_ATTR = "affine";
	public static final String SPACING_ATTR = "spacing";
	public static final String FORWARD_ATTR = "dfield";
	public static final String INVERSE_ATTR = "invdfield";


    /**
     * Saves forward and inverse deformation fields into the default n5 datasets.
     *
     * @param n5Writer
     * @param affine
     * @param forwardDfield
     * @param fwdspacing the pixel spacing (resolution) of the forward deformation field
     * @param inverseDfield
     * @param invspacing the pixel spacing (resolution) of the inverse deformation field
     * @param blockSize 
     * @param compression
     */ 
	public static final <T extends NativeType<T> & RealType<T>> void save(
			final N5Writer n5Writer,
			final AffineGet affine,
			final RandomAccessibleInterval< T > forwardDfield,
			final double[] fwdspacing,
			final RandomAccessibleInterval< T > inverseDfield,
			final double[] invspacing,
			final int[] blockSize,
			final Compression compression ) throws IOException
	{
	    save( n5Writer, FORWARD_ATTR, affine, forwardDfield, fwdspacing, blockSize, compression );
	    save( n5Writer, INVERSE_ATTR, affine.inverse(), inverseDfield, invspacing, blockSize, compression );
	}

    /**
     * Saves an affine transform and deformation field into a specified n5 dataset.
     *
     *
     * @param n5Writer
     * @param dataset
     * @param affine
     * @param dfield
     * @param spacing the pixel spacing (resolution) of the deformation field
     * @param blockSize
     * @param compression
     */
	public static final <T extends NativeType<T> & RealType<T>> void save(
			final N5Writer n5Writer,
			final String dataset,
			final AffineGet affine,
			final RandomAccessibleInterval< T > dfield,
			final double[] spacing,
			final int[] blockSize,
			final Compression compression ) throws IOException
	{
		N5Utils.save( dfield, n5Writer, dataset, blockSize, compression );

        if( affine != null )
            saveAffine( affine, n5Writer, dataset );

		if( spacing != null )
			n5Writer.setAttribute( dataset, SPACING_ATTR, spacing );
	}

    /**
     * Saves an affine transform and quantized deformation field into a specified n5 dataset.
     *
     * The deformation field here is saved as an {@link IntegerType}
     * which could compress better in some cases.  The multiplier from
     * original values to compressed values is chosen as the smallest
     * value that keeps the error (L2) between quantized and original vectors. 
     *
     * @param n5Writer
     * @param dataset
     * @param affine
     * @param dfield
     * @param spacing
     * @param blockSize
     * @param compression
     * @param outputType
     * @param maxError
     */
	public static final <T extends NativeType<T> & RealType<T>, Q extends NativeType<Q> & IntegerType<Q>> void save(
			final N5Writer n5Writer,
			final String dataset,
			final AffineGet affine,
			final RandomAccessibleInterval< T > dfield,
			final double[] spacing,
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

    /**
     * Saves an affine transform as an attribute associated with an n5
     * dataset.
     *
     * @param affine
     * @param n5Writer
     * @param dataset
     */
	public static final void saveAffine(
			final AffineGet affine,
			final N5Writer n5Writer,
			final String dataset ) throws IOException
	{
		if( affine != null )
			n5Writer.setAttribute( dataset, AFFINE_ATTR,  affine.getRowPackedCopy() );
	}

    /**
     * Saves an affine transform and quantized deformation field into a specified n5 dataset.
     *
     * The deformation field here is saved as an {@link IntegerType}
     * which could compress better in some cases.  The multiplier from
     * original values to compressed values is chosen as the smallest
     * value that keeps the error (L2) between quantized and original vectors. 
     *
     * @param n5Writer
     * @param dataset
     * @param source
     * @param blockSize
     * @param compression
     * @param outputType
     * @param maxError
     */
	public static final <T extends RealType<T>, Q extends NativeType<Q> & IntegerType<Q>> void saveQuantized(
			final N5Writer n5Writer,
			final String dataset,
			final RandomAccessibleInterval<T> source,
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
						output.setInteger( Math.round( input.getRealDouble() / m ));
					}
				}, 
				outputType.copy());

        N5Utils.save( source_quant, n5Writer, dataset, blockSize, compression);
		n5Writer.setAttribute( dataset, MULTIPLIER_ATTR, m );
	}

    /**
     * Opens an {@link InvertibleRealTransform} from an n5 object if
     * possible.
     *
     * @param n5
     * @param defaultType
     * @param interpolator
     * @return the invertible transformation
     */
	public static final <T extends RealType<T> & NativeType<T>> ExplicitInvertibleRealTransform openInvertible( 
			final N5Reader n5,
			final T defaultType,
			final InterpolatorFactory< T, RandomAccessible<T> > interpolator ) throws Exception
	{
		return new ExplicitInvertibleRealTransform(
                open( n5, FORWARD_ATTR, false, defaultType, interpolator),
                open( n5, INVERSE_ATTR, true, defaultType, interpolator));
	}

    /**
     * Opens a {@link RealTransform} from an n5 dataset as a
     * displacement field.  The resulting transform is the concatenation
     * of an affine transform and a {@link DeformationFieldTransform}.
     *
     * @param n5
     * @param dataset
     * @param inverse
     * @param defaultType
     * @param interpolator
     * @return the transformation
     */
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

    /**
     * Returns an {@link AffineGet} transform from pixel space to
     * physical space, for the given n5 dataset, if present, null
     * otherwise.
     *
     * @param n5
     * @param dataset
     * @return the affine transform
     */
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

    /**
     * Returns and affine transform stored as an attribute in an n5 dataset.
     *
     * @param n5
     * @param dataset
     * @return the affine
     */
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

    /**
     * Returns a deformation field from the given n5 dataset.
     *
     * If the data is an {@link IntegerType}, returns an un-quantized
     * view of the dataset, otherwise, returns the raw {@link
     * RandomAccessibleInterval}.
     *
     * @param n5
     * @param dataset
     * @param defaultType
     * @return the deformation field as a RandomAccessibleInterval
     */
	@SuppressWarnings( "unchecked" )
	public static final <T extends NativeType<T> & RealType<T>, Q extends NativeType<Q> & IntegerType<Q>> RandomAccessibleInterval< T > openField( 
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

    /**
     * Returns a deformation field in physical coordinates as a {@link
     * RealRandomAccessible} from an n5 dataset.
     *
     * Internally, opens the given n5 dataset as a {@link
     * RandomAccessibleInterval}, un-quantizes if necessary, uses
     * the input {@link InterpolatorFactory} for interpolation, and
     * transforms to physical coordinates using the pixel spacing stored
     * in the "spacing" attribute, if present.
     *
     * @param n5
     * @param dataset
     * @param interpolator
     * @param defaultType
     * @return the deformation field as a RealRandomAccessible
     */
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

    /**
     * Opens a transform from an n5 dataset using linear interpolation
     * for the deformation field.
     *
     * @param n5
     * @param dataset
     * @param inverse
     * @return the transform
     */
	public static < T extends NativeType< T > & RealType< T > > RealTransform open(
			final N5Reader n5, final String dataset, boolean inverse ) throws Exception
	{
		return open( n5, dataset, inverse, new FloatType(), new NLinearInterpolatorFactory<FloatType>());
	}

    /**
     * Returns a deformation field as a {@link RandomAccessibleInterval}, ensuring that
     * the vector is stored in the last dimension.
     *
     * @param n5
     * @param dataset
     * @param defaultType
     * @return the deformation field
     */
	public static final < T extends RealType<T> & NativeType<T> > RandomAccessibleInterval< T > openRaw(
			final N5Reader n5,
			final String dataset,
			final T defaultType ) throws Exception
	{
        RandomAccessibleInterval< T > src = N5Utils.open( n5, dataset, defaultType  );
        return vectorAxisLast( src );
	}

    /**
     * Open a quantized (integer) {@link RandomAccessibleInterval} from an n5
     * dataset.
     *
     * @param n5
     * @param dataset
     * @param defaultQuantizedType
     * @param defaultType
     * @return the un-quantized data
     */
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

    /**
     * Returns a deformation field as a {@link RandomAccessibleInterval}
     * with the vector stored in the last dimension.
     *
     * @param source
     * @return the possibly permuted deformation field
     * @throws Exception
     */
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
	
    /**
     * Returns a deformation field as a {@link RandomAccessibleInterval}
     * with the vector stored in the first dimension.
     *
     * @param source
     * @return the possibly permuted deformation field
     * @throws Exception
     */
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

    /**
     * Permutes the dimensions of a {@link RandomAccessibleInterval}
     * using the given permutation vector, where the ith value in p
     * gives destination of the ith input dimension in the output. 
     *
     * @param source the source data
     * @param p the permutation
     * @return the permuted source
     */
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

    /**
     * Returns a two element double array in which the first and second elements
     * store the minimum and maximum values of the input {@link
     * IterableInterval}, respectively.
     *
     * @param img the iterable interval
     * @return the min and max values stored in a double array
     */
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

package org.janelia.saalfeldlab.n5.imglib2;

import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5Writer;

import net.imglib2.Cursor;
import net.imglib2.IterableInterval;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.converter.Converter;
import net.imglib2.converter.Converters;
import net.imglib2.transform.integer.MixedTransform;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.util.Util;
import net.imglib2.view.IntervalView;
import net.imglib2.view.MixedTransformView;
import net.imglib2.view.Views;

public class N5DisplacementField
{
	public static final String MULTIPLIER_ATTR = "quantization_multiplier";


	public static final <T extends RealType<T>, Q extends NativeType<Q> & RealType<Q>> void saveQuantized(
			RandomAccessibleInterval<T> source,
			final N5Writer n5Writer,
			final String dataset,
			final int[] blockSize,
			final Compression compression,
			final Q outputType,
            final double maxError ) throws Exception
	{
		RandomAccessibleInterval< T > source_permuted = vectorAxisFirst( source );
		
        // the value that should be mapped to the maxValue of the
        // quantization type in order to ensure quantization error is
        // below maxError
		double max = 2 * maxError * ( outputType.getMaxValue() - 1 );
		double m = (outputType.getMaxValue() - 1) / max;
		
//		System.out.println( "m : " + m );

		RandomAccessibleInterval< Q > source_quant = Converters.convert(
				source_permuted, 
				new Converter<T, Q>()
				{
					@Override
					public void convert(T input, Q output)
					{
						output.setReal( Math.round( input.getRealDouble() * m ));
					}
				}, 
				outputType.copy());

        N5Utils.save( source_quant, n5Writer, dataset, blockSize, compression);
		n5Writer.setAttribute( dataset, MULTIPLIER_ATTR, 1 / m );
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
	
	public static final < T extends RealType< T > > RandomAccessibleInterval< T > vectorAxisLast3( RandomAccessibleInterval< T > source ) throws Exception
	{
		final int n = source.numDimensions();

		if ( source.dimension( n - 1 ) == (n - 1) )
			return source;
		else if ( source.dimension( 0 ) == (n - 1) )
		{
			Views.permute( Views.permute( Views.permute( source, 0, 3 ), 0, 1 ), 1, 2 );
		}
		throw new Exception( 
				String.format( "Displacement fields must store vector components in the first or last dimension. " + 
						"Found a %d-d volume; expect size [%d,...] or [...,%d]", n, ( n - 1 ), ( n - 1 ) ) );
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
	
	public static final < T extends RealType< T > > RandomAccessibleInterval< T > vectorAxisFirst3( RandomAccessibleInterval< T > source ) throws Exception
	{
		final int n = source.numDimensions();
		if ( source.dimension( 0 ) == (n - 1) )
			return source;
		else if ( source.dimension( n - 1 ) == (n - 1) )
		{
			Views.permute( Views.permute( Views.permute( source, 0, 3 ), 1, 3 ), 2, 3 );
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

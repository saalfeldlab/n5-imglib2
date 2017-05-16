package org.janelia.saalfeldlab.n5.cache;

import java.util.function.Function;

import net.imglib2.Cursor;
import net.imglib2.Interval;
import net.imglib2.RandomAccessible;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.basictypeaccess.volatiles.array.VolatileByteArray;
import net.imglib2.img.basictypeaccess.volatiles.array.VolatileDoubleArray;
import net.imglib2.img.basictypeaccess.volatiles.array.VolatileFloatArray;
import net.imglib2.img.basictypeaccess.volatiles.array.VolatileIntArray;
import net.imglib2.img.basictypeaccess.volatiles.array.VolatileLongArray;
import net.imglib2.img.basictypeaccess.volatiles.array.VolatileShortArray;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.complex.ComplexDoubleType;
import net.imglib2.type.numeric.complex.ComplexFloatType;
import net.imglib2.type.numeric.integer.ByteType;
import net.imglib2.type.numeric.integer.IntType;
import net.imglib2.type.numeric.integer.LongType;
import net.imglib2.type.numeric.integer.ShortType;
import net.imglib2.type.numeric.integer.UnsignedByteType;
import net.imglib2.type.numeric.integer.UnsignedIntType;
import net.imglib2.type.numeric.integer.UnsignedLongType;
import net.imglib2.type.numeric.integer.UnsignedShortType;
import net.imglib2.type.numeric.real.DoubleType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.Fraction;
import net.imglib2.util.Intervals;
import net.imglib2.view.Views;

public class TransformedAccessibleLoader< T extends NativeType< T >, A > implements Function< Interval, A >
{

	public static interface AccessGenerator< T extends NativeType< T >, A >
	{
		public ArrayImg< T, A > createAccess( long[] dim );

		default public ArrayImg< T, A > createAccess( final Interval interval )
		{
			return createAccess( Intervals.dimensionsAsLongArray( interval ) );
		}

	}

	private final RandomAccessible< T > source;

	private final AccessGenerator< T, A > accessGenerator;

	public TransformedAccessibleLoader( final RandomAccessible< T > source, final T t ) {
		this( source, defaultArrayAccessGenerator( t ), t );
	}

	public TransformedAccessibleLoader( final RandomAccessible< T > source, final AccessGenerator< T, A > accessGenerator, final T t )
	{
		super();
		this.source = source;
		this.accessGenerator = accessGenerator;
	}

	@Override
	public A apply( final Interval interval )
	{
		final ArrayImg< T, A > target = accessGenerator.createAccess( interval );
		for ( Cursor< T > s = Views.flatIterable( Views.interval( source, interval ) ).cursor(), t = target.cursor(); s.hasNext(); )
			t.next().set( s.next() );
		return target.update( null );
	}

	@SuppressWarnings( "unchecked" )
	public static < T extends NativeType< T >, A > AccessGenerator< T, A > defaultArrayAccessGenerator( final T t )
	{
		final Fraction entitiesPerPixel = t.getEntitiesPerPixel();

		if ( t instanceof ByteType || t instanceof UnsignedByteType )
		{
			return ( dim ) -> {
				final VolatileByteArray store = new VolatileByteArray( ( int ) entitiesPerPixel.mulCeil( Intervals.numElements( dim ) ), true );
				return ( ArrayImg< T, A > ) ( t instanceof ByteType ? ArrayImgs.bytes( store, dim ) : ArrayImgs.unsignedBytes( store, dim ) );
			};
		}

		if ( t instanceof ShortType || t instanceof UnsignedShortType )
		{
			return ( dim ) -> {
				final VolatileShortArray store = new VolatileShortArray( ( int ) entitiesPerPixel.mulCeil( Intervals.numElements( dim ) ), true );
				return ( ArrayImg< T, A > ) ( t instanceof ShortType ? ArrayImgs.shorts( store, dim ) : ArrayImgs.unsignedShorts( store, dim ) );
			};
		}

		if ( t instanceof IntType || t instanceof UnsignedIntType )
		{
			return ( dim ) -> {
				final VolatileIntArray store = new VolatileIntArray( ( int ) entitiesPerPixel.mulCeil( Intervals.numElements( dim ) ), true );
				return ( ArrayImg< T, A > ) ( t instanceof IntType ? ArrayImgs.ints( store, dim ) : ArrayImgs.unsignedInts( store, dim ) );
			};
		}

		if ( t instanceof LongType || t instanceof UnsignedLongType )
		{
			return ( dim ) -> {
				final VolatileLongArray store = new VolatileLongArray( ( int ) entitiesPerPixel.mulCeil( Intervals.numElements( dim ) ), true );
				return ( ArrayImg< T, A > ) ( t instanceof LongType ? ArrayImgs.longs( store, dim ) : ArrayImgs.unsignedLongs( store, dim ) );
			};
		}

		if ( t instanceof FloatType || t instanceof ComplexFloatType )
		{
			return ( dim ) -> {
				final VolatileFloatArray store = new VolatileFloatArray( ( int ) entitiesPerPixel.mulCeil( Intervals.numElements( dim ) ), true );
				return ( ArrayImg< T, A > ) ( t instanceof LongType ? ArrayImgs.floats( store, dim ) : ArrayImgs.complexFloats( store, dim ) );
			};
		}

		if ( t instanceof DoubleType || t instanceof ComplexDoubleType )
		{
			return ( dim ) -> {
				final VolatileDoubleArray store = new VolatileDoubleArray( ( int ) entitiesPerPixel.mulCeil( Intervals.numElements( dim ) ), true );
				return ( ArrayImg< T, A > ) ( t instanceof LongType ? ArrayImgs.doubles( store, dim ) : ArrayImgs.complexDoubles( store, dim ) );
			};
		}

		throw new IllegalArgumentException( "No default access generator avaialable for type " + t.getClass().getName() );
	}

}

package org.janelia.saalfeldlab.n5.imglib2;

import net.imglib2.Cursor;
import net.imglib2.Interval;
import net.imglib2.RandomAccessible;
import net.imglib2.cache.img.CellLoader;
import net.imglib2.cache.img.SingleCellArrayImg;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.type.NativeType;
import net.imglib2.util.Intervals;
import net.imglib2.view.Views;

public class RandomAccessibleLoader< T extends NativeType< T > > implements CellLoader< T >
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

	public RandomAccessibleLoader( final RandomAccessible< T > source )
	{
		super();
		this.source = source;
	}

	@Override
	public void load( final SingleCellArrayImg< T, ? > interval )
	{
		for ( Cursor< T > s = Views.flatIterable( Views.interval( source, interval ) ).cursor(), t = interval.cursor(); s.hasNext(); )
			t.next().set( s.next() );
	}
}
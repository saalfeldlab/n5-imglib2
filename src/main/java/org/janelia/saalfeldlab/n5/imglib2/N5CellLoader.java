package org.janelia.saalfeldlab.n5.imglib2;

import java.io.IOException;
import java.util.Arrays;
import java.util.function.BiConsumer;

import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5;

import net.imglib2.Cursor;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.cache.img.CellLoader;
import net.imglib2.img.Img;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.basictypeaccess.array.LongArray;
import net.imglib2.type.NativeType;
import net.imglib2.type.Type;
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
import net.imglib2.util.Intervals;
import net.imglib2.util.Util;
import net.imglib2.view.Views;

public class N5CellLoader< T extends NativeType< T > > implements CellLoader< T >
{
	private final N5 n5;

	private final String dataset;

	private final int[] cellDimensions;

	private final DatasetAttributes attributes;

	private final BiConsumer< Img< T >, DataBlock< ? > > copyFromBlock;

	public N5CellLoader( final N5 n5, final String dataset, final int[] cellDimensions ) throws IOException
	{
		this( n5, dataset, cellDimensions, defaultCopyFromBlock() );
	}

	public N5CellLoader( final N5 n5, final String dataset, final int[] cellDimensions, final BiConsumer< Img< T >, DataBlock< ? > > copyFromBlock ) throws IOException
	{
		super();
		this.n5 = n5;
		this.dataset = dataset;
		this.cellDimensions = cellDimensions;
		this.attributes = n5.getDatasetAttributes( dataset );
		this.copyFromBlock = copyFromBlock;
		if ( ! Arrays.equals( this.cellDimensions, attributes.getBlockSize() ) )
			throw new RuntimeException( "Cell dimensions inconsistent! " + " " + Arrays.toString( cellDimensions ) + " " + Arrays.toString( attributes.getBlockSize() ) );
	}

	@Override
	public void load( final Img< T > interval )
	{
		final long[] gridPosition = new long[ interval.numDimensions() ];
		for ( int d = 0; d < gridPosition.length; ++d )
			gridPosition[ d ] = interval.min( d ) / cellDimensions[ d ];
		DataBlock< ? > block;
		try
		{
			block = n5.readBlock( dataset, attributes, gridPosition );
		}
		catch ( final IOException e )
		{
			throw new RuntimeException( e );
		}

		copyFromBlock.accept( interval, block );
	}

	public static < T extends Type< T > > void burnIn( final RandomAccessibleInterval< T > source, final RandomAccessibleInterval< T > target )
	{
		for ( Cursor< T > s = Views.flatIterable( source ).cursor(), t = Views.flatIterable( target ).cursor(); t.hasNext(); )
			t.next().set( s.next() );
	}

	@SuppressWarnings( "unchecked" )
	public static < T extends NativeType< T > > BiConsumer< Img< T >, DataBlock< ? > > defaultCopyFromBlock()
	{
		final BiConsumer< Img< T >, DataBlock< ? > > copyFromBlock = ( img, block ) -> {
			final T t = Util.getTypeFromInterval( img );
			final long[] dim = Intervals.dimensionsAsLongArray( img );

			if ( t instanceof ByteType )
				burnIn( (Img< T > )ArrayImgs.bytes( (byte[]) block.getData(), dim ), img );
			else if ( t instanceof ShortType )
				burnIn( ( Img< T > ) ArrayImgs.shorts( ( short[] ) block.getData(), dim ), img );
			else if ( t instanceof IntType )
				burnIn( ( Img< T > ) ArrayImgs.ints( ( int[] ) block.getData(), dim ), img );
			else if ( t instanceof LongType )
				burnIn( ( Img< T > ) ArrayImgs.longs( ( long[] ) block.getData(), dim ), img );
			else if ( t instanceof UnsignedByteType )
				burnIn( ( Img< T > ) ArrayImgs.unsignedBytes( ( byte[] ) block.getData(), dim ), img );
			else if ( t instanceof UnsignedShortType )
				burnIn( ( Img< T > ) ArrayImgs.unsignedShorts( ( short[] ) block.getData(), dim ), img );
			else if ( t instanceof UnsignedIntType )
				burnIn( ( Img< T > ) ArrayImgs.unsignedInts( ( int[] ) block.getData(), dim ), img );
			else if ( t instanceof UnsignedLongType )
				/* TODO missing factory method in ArrayImgs, replace when ImgLib2 updated */
				burnIn( ( Img< T > ) ArrayImgs.unsignedLongs( new LongArray( ( long[] ) block.getData() ), dim ), img );
			else if ( t instanceof FloatType )
				burnIn( ( Img< T > ) ArrayImgs.floats( ( float[] ) block.getData(), dim ), img );
			else if ( t instanceof DoubleType )
				burnIn( ( Img< T > ) ArrayImgs.doubles( ( double[] ) block.getData(), dim ), img );
			else if ( t instanceof ComplexFloatType )
				burnIn( ( Img< T > ) ArrayImgs.complexFloats( ( float[] ) block.getData(), dim ), img );
			else if ( t instanceof ComplexDoubleType )
				burnIn( ( Img< T > ) ArrayImgs.complexDoubles( ( double[] ) block.getData(), dim ), img );
			else
				throw new IllegalArgumentException( "Type " + t.getClass().getName() + " not supported!" );
		};

		return copyFromBlock;
	}
}

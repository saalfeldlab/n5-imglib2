package org.janelia.saalfeldlab.n5.imglib2;

import java.io.IOException;
import java.util.Arrays;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5Reader;

import net.imglib2.Cursor;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.cache.img.CellLoader;
import net.imglib2.cache.img.SingleCellArrayImg;
import net.imglib2.img.Img;
import net.imglib2.type.NativeType;
import net.imglib2.type.Type;
import net.imglib2.type.numeric.integer.GenericByteType;
import net.imglib2.type.numeric.integer.GenericIntType;
import net.imglib2.type.numeric.integer.GenericLongType;
import net.imglib2.type.numeric.integer.GenericShortType;
import net.imglib2.type.numeric.real.DoubleType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.view.Views;

public class N5CellLoader< T extends NativeType< T > > implements CellLoader< T >
{
	private final N5Reader n5;

	private final String dataset;

	private final int[] cellDimensions;

	private final DatasetAttributes attributes;

	private final BiConsumer< Img< T >, DataBlock< ? > > copyFromBlock;

	private final Consumer< Img< T > > blockNotFoundHandler;

	/**
	 *
	 * Calls
	 * {@link N5CellLoader#N5CellLoader(N5Reader, String, int[], Consumer)} with
	 * {@code blockNotFoundHandler} defaulting to
	 * {@link N5CellLoader#noOpConsumer()}.
	 *
	 * @param n5
	 * @param dataset
	 * @param cellDimensions
	 * @throws IOException
	 */
	public N5CellLoader( final N5Reader n5, final String dataset, final int[] cellDimensions ) throws IOException
	{
		this( n5, dataset, cellDimensions, noOpConsumer() );
	}

	/**
	 *
	 * @param n5
	 * @param dataset
	 * @param cellDimensions
	 * @param blockNotFoundHandler
	 *            Sets block contents if the appropriate {@link N5Reader}
	 *            returns {@code null} for that block.
	 * @throws IOException
	 */
	public N5CellLoader( final N5Reader n5, final String dataset, final int[] cellDimensions, final Consumer< Img< T > > blockNotFoundHandler ) throws IOException
	{
		super();
		this.n5 = n5;
		this.dataset = dataset;
		this.cellDimensions = cellDimensions;
		this.attributes = n5.getDatasetAttributes( dataset );
		this.copyFromBlock = createCopy( attributes.getDataType() );
		this.blockNotFoundHandler = blockNotFoundHandler;
		if ( ! Arrays.equals( this.cellDimensions, attributes.getBlockSize() ) )
			throw new RuntimeException( "Cell dimensions inconsistent! " + " " + Arrays.toString( cellDimensions ) + " " + Arrays.toString( attributes.getBlockSize() ) );
	}

	@Override
	public void load( final SingleCellArrayImg< T, ? > cell )
	{
		final long[] gridPosition = new long[ cell.numDimensions() ];
		for ( int d = 0; d < gridPosition.length; ++d )
			gridPosition[ d ] = cell.min( d ) / cellDimensions[ d ];
		final DataBlock< ? > block;
		try
		{
			block = n5.readBlock( dataset, attributes, gridPosition );
		}
		catch ( final IOException e )
		{
			throw new RuntimeException( e );
		}

		if ( block == null )
			blockNotFoundHandler.accept( cell );
		else
			copyFromBlock.accept( cell, block );

	}

	public static < T extends Type< T > > void burnIn( final RandomAccessibleInterval< T > source, final RandomAccessibleInterval< T > target )
	{
		for ( Cursor< T > s = Views.flatIterable( source ).cursor(), t = Views.flatIterable( target ).cursor(); t.hasNext(); )
			t.next().set( s.next() );
	}

	/**
	 * Copies data from source into target and tests whether all values equal a
	 * reference value.
	 *
	 * @param source
	 * @param target
	 * @return
	 */
	public static < T extends Type< T > > boolean burnInTestAllEqual(
			final RandomAccessibleInterval< T > source,
			final RandomAccessibleInterval< T > target,
			final T reference )
	{
		boolean equal = true;
		for ( Cursor< T > s = Views.flatIterable( source ).cursor(), t = Views.flatIterable( target ).cursor(); t.hasNext(); )
		{
			final T ts = s.next();
			equal &= reference.valueEquals( ts );
			t.next().set( ts );
		}

		return equal;
	}

	public static < T extends NativeType< T > > BiConsumer< Img< T >, DataBlock< ? > > createCopy( final DataType dataType )
	{
		switch ( dataType )
		{
		case INT8:
		case UINT8:
			return ( a, b ) -> {
				final byte[] data = ( byte[] )b.getData();
				@SuppressWarnings( "unchecked" )
				final Cursor< ? extends GenericByteType< ? > > c = ( Cursor< ? extends GenericByteType< ? > > )a.cursor();
				for ( int i = 0; i < data.length; ++i )
					c.next().setByte( data[ i ] );
			};
		case INT16:
		case UINT16:
			return ( a, b ) -> {
				final short[] data = ( short[] )b.getData();
				@SuppressWarnings( "unchecked" )
				final Cursor< ? extends GenericShortType< ? > > c = ( Cursor< ? extends GenericShortType< ? > > )a.cursor();
				for ( int i = 0; i < data.length; ++i )
					c.next().setShort( data[ i ] );
			};
		case INT32:
		case UINT32:
			return ( a, b ) -> {
				final int[] data = ( int[] )b.getData();
				@SuppressWarnings( "unchecked" )
				final Cursor< ? extends GenericIntType< ? > > c = ( Cursor< ? extends GenericIntType< ? > > )a.cursor();
				for ( int i = 0; i < data.length; ++i )
					c.next().setInt( data[ i ] );
			};
		case INT64:
		case UINT64:
			return ( a, b ) -> {
				final long[] data = ( long[] )b.getData();
				@SuppressWarnings( "unchecked" )
				final Cursor< ? extends GenericLongType< ? > > c = ( Cursor< ? extends GenericLongType< ? > > )a.cursor();
				for ( int i = 0; i < data.length; ++i )
					c.next().setLong( data[ i ] );
			};
		case FLOAT32:
			return ( a, b ) -> {
				final float[] data = ( float[] )b.getData();
				@SuppressWarnings( "unchecked" )
				final Cursor< ? extends FloatType > c = ( Cursor< ? extends FloatType > )a.cursor();
				for ( int i = 0; i < data.length; ++i )
					c.next().set( data[ i ] );
			};
		case FLOAT64:
			return ( a, b ) -> {
				final double[] data = ( double[] )b.getData();
				@SuppressWarnings( "unchecked" )
				final Cursor< ? extends DoubleType > c = ( Cursor< ? extends DoubleType > )a.cursor();
				for ( int i = 0; i < data.length; ++i )
					c.next().set( data[ i ] );
			};
		default:
			throw new IllegalArgumentException( "Type " + dataType.name() + " not supported!" );
		}
	}

	/**
	 *
	 * @return {@link Consumer} that does not do anything.
	 */
	public static < T > Consumer< T > noOpConsumer()
	{
		return t -> {};
	}

	/**
	 *
	 * @param defaultValue
	 * @return {@link Consumer} that sets all values of its argument to
	 *         {@code defaultValue}.
	 */
	public static < T extends Type< T >, I extends RandomAccessibleInterval< T > > Consumer< I > setToDefaultValue( T defaultValue )
	{
		return rai -> Views.iterable( rai ).forEach( pixel -> pixel.set( defaultValue ) );
	}
}

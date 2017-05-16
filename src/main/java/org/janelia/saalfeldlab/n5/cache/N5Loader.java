package org.janelia.saalfeldlab.n5.cache;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.function.Function;

import org.janelia.saalfeldlab.n5.AbstractDataBlock;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5;

import bdv.util.BdvFunctions;
import bdv.util.BdvStackSource;
import net.imglib2.Interval;
import net.imglib2.cache.img.DiskCellCache;
import net.imglib2.cache.img.PrimitiveType;
import net.imglib2.cache.queue.BlockingFetchQueues;
import net.imglib2.cache.queue.FetcherThreads;
import net.imglib2.cache.util.IntervalKeyLoaderAsLongKeyLoader;
import net.imglib2.img.Img;
import net.imglib2.img.basictypeaccess.array.ByteArray;
import net.imglib2.img.basictypeaccess.array.CharArray;
import net.imglib2.img.basictypeaccess.array.DoubleArray;
import net.imglib2.img.basictypeaccess.array.FloatArray;
import net.imglib2.img.basictypeaccess.array.IntArray;
import net.imglib2.img.basictypeaccess.array.LongArray;
import net.imglib2.img.basictypeaccess.array.ShortArray;
import net.imglib2.img.basictypeaccess.volatiles.array.VolatileByteArray;
import net.imglib2.img.basictypeaccess.volatiles.array.VolatileCharArray;
import net.imglib2.img.basictypeaccess.volatiles.array.VolatileDoubleArray;
import net.imglib2.img.basictypeaccess.volatiles.array.VolatileFloatArray;
import net.imglib2.img.basictypeaccess.volatiles.array.VolatileIntArray;
import net.imglib2.img.basictypeaccess.volatiles.array.VolatileLongArray;
import net.imglib2.img.basictypeaccess.volatiles.array.VolatileShortArray;
import net.imglib2.img.cell.CellGrid;
import net.imglib2.type.numeric.integer.UnsignedShortType;
import net.imglib2.type.volatiles.VolatileUnsignedShortType;
import net.imglib2.util.Pair;

public class N5Loader< A > implements Function< Interval, A >
{


	private final N5 n5;

	private final String dataset;

	private final int[] cellDimensions;

	private final DatasetAttributes attributes;

	private final Function< AbstractDataBlock< ? >, A > accessGenerator;

	public N5Loader( final N5 n5, final String dataset, final int[] cellDimensions, final Function< AbstractDataBlock< ? >, A > accessGenerator ) throws IOException
	{
		super();
		this.n5 = n5;
		this.dataset = dataset;
		this.cellDimensions = cellDimensions;
		this.accessGenerator = accessGenerator;
		this.attributes = n5.getDatasetAttributes( dataset );
		if ( ! Arrays.equals( this.cellDimensions, attributes.getBlockSize() ) )
			throw new RuntimeException( "Cell dimensions inconsistent! " + " " + Arrays.toString( cellDimensions ) + " " + Arrays.toString( attributes.getBlockSize() ) );
	}

	@Override
	public A apply( final Interval interval )
	{
		final long[] gridPosition = new long[ interval.numDimensions() ];
		for ( int d = 0; d < gridPosition.length; ++d )
			gridPosition[ d ] = interval.min( d ) / cellDimensions[ d ];
		AbstractDataBlock< ? > block;
		try
		{
			block = n5.readBlock( dataset, attributes, gridPosition );
		}
		catch ( final IOException e )
		{
			throw new RuntimeException( e );
		}

		final A access = accessGenerator.apply( block );
		return access;
	}

	@SuppressWarnings( "unchecked" )
	public static < A > Function< AbstractDataBlock< ? >, A > defaultArrayAccessGenerator( final boolean loadVolatile )
	{
		return block -> {
			final Object blockData = block.getData();
			if ( blockData instanceof byte[] )
				return ( A ) ( loadVolatile ? new VolatileByteArray( ( byte[] ) blockData, true ) : new ByteArray( ( byte[] ) blockData ) );
			if ( blockData instanceof char[] )
				return ( A ) ( loadVolatile ? new VolatileCharArray( ( char[] ) blockData, true ) : new CharArray( ( char[] ) blockData ) );
			else if ( blockData instanceof short[] )
				return ( A ) ( loadVolatile ? new VolatileShortArray( ( short[] ) blockData, true ) : new ShortArray( ( short[] ) blockData ) );
			else if ( blockData instanceof int[] )
				return ( A ) ( loadVolatile ? new VolatileIntArray( ( int[] ) blockData, true ) : new IntArray( ( int[] ) blockData ) );
			else if ( blockData instanceof long[] )
				return ( A ) ( loadVolatile ? new VolatileLongArray( ( long[] ) blockData, true ) : new LongArray( ( long[] ) blockData ) );
			else if ( blockData instanceof float[] )
				return ( A ) ( loadVolatile ? new VolatileFloatArray( ( float[] ) blockData, true ) : new FloatArray( ( float[] ) blockData ) );
			else if ( blockData instanceof double[] )
				return ( A ) ( loadVolatile ? new VolatileDoubleArray( ( double[] ) blockData, true ) : new DoubleArray( ( double[] ) blockData ) );
			else
				throw new RuntimeException( "Do not support this class: " + blockData.getClass().getName() );
		};
	}


	public static void main( final String[] args ) throws IOException
	{
		final String n5path = "/data/hanslovskyp/n5-tests/davi-toy-set";
		final String dataset = "excerpt";
		final N5 n5 = new N5( n5path );
		final DatasetAttributes attr = n5.getDatasetAttributes( dataset );
		final long[] dim = attr.getDimensions();
		final int[] cellSize = attr.getBlockSize();

		final CellGrid grid = new CellGrid( dim, cellSize );

		System.out.println( attr.getNumDimensions() + " " + attr.getDataType() + " " + attr.getCompressionType() + " " + Arrays.toString( attr.getDimensions() ) + " " + Arrays.toString( attr.getBlockSize() ) );

		final int numProc = Runtime.getRuntime().availableProcessors();
		final int maxNumLevels = 1;
		final int numFetcherThreads = numProc - 1;
		final BlockingFetchQueues< Callable< ? > > queue = new BlockingFetchQueues<>( maxNumLevels );
		new FetcherThreads( queue, numFetcherThreads );

		final N5Loader< VolatileShortArray > loader = new N5Loader<>( n5, dataset, cellSize, defaultArrayAccessGenerator( true ) );
		final IntervalKeyLoaderAsLongKeyLoader< VolatileShortArray > longKeyLoader = new IntervalKeyLoaderAsLongKeyLoader<>( grid, loader );

		final Pair< Img< UnsignedShortType >, Img< VolatileUnsignedShortType > > imgs =
				CacheUtil.createImgAndVolatileImgFromCacheLoader( grid, queue, longKeyLoader, new UnsignedShortType(), new VolatileUnsignedShortType(), PrimitiveType.SHORT, DiskCellCache.createTempDirectory( "blocks", true ) );

		final BdvStackSource< VolatileUnsignedShortType > bdv = BdvFunctions.show( imgs.getB(), "volatile!" );
		bdv.getBdvHandle().getSetupAssignments().getMinMaxGroups().get( 0 ).setRange( 0, 255 );

	}

}

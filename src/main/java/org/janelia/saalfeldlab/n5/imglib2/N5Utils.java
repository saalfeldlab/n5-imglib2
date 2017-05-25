/**
 *
 */
package org.janelia.saalfeldlab.n5.imglib2;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

import org.janelia.saalfeldlab.n5.CompressionType;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5;

import net.imglib2.Interval;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.cache.Cache;
import net.imglib2.cache.img.ArrayDataAccessFactory;
import net.imglib2.cache.img.CachedCellImg;
import net.imglib2.cache.img.DiskCachedCellImg;
import net.imglib2.cache.img.DiskCachedCellImgFactory;
import net.imglib2.cache.img.DiskCachedCellImgOptions;
import net.imglib2.cache.img.LoadedCellCacheLoader;
import net.imglib2.cache.img.PrimitiveType;
import net.imglib2.cache.ref.SoftRefLoaderCache;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.basictypeaccess.array.ByteArray;
import net.imglib2.img.basictypeaccess.array.DoubleArray;
import net.imglib2.img.basictypeaccess.array.FloatArray;
import net.imglib2.img.basictypeaccess.array.IntArray;
import net.imglib2.img.basictypeaccess.array.LongArray;
import net.imglib2.img.basictypeaccess.array.ShortArray;
import net.imglib2.img.cell.Cell;
import net.imglib2.img.cell.CellGrid;
import net.imglib2.type.NativeType;
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

/**
 * @author Stephan Saalfeld &lt;saalfelds@janelia.hhmi.org&gt;
 *
 */
public class N5Utils {

	private N5Utils() {}

	public static final < T extends NativeType< T > > DataType dataType( final T type )
	{
		if ( DoubleType.class.isInstance( type ) )
			return DataType.FLOAT64;
		if ( FloatType.class.isInstance( type ) )
			return DataType.FLOAT32;
		if ( LongType.class.isInstance( type ) )
			return DataType.INT64;
		if ( UnsignedLongType.class.isInstance( type ) )
			return DataType.UINT64;
		if ( IntType.class.isInstance( type ) )
			return DataType.INT32;
		if ( UnsignedIntType.class.isInstance( type ) )
			return DataType.UINT32;
		if ( ShortType.class.isInstance( type ) )
			return DataType.INT16;
		if ( UnsignedShortType.class.isInstance( type ) )
			return DataType.UINT16;
		if ( ByteType.class.isInstance( type ) )
			return DataType.INT8;
		if ( UnsignedByteType.class.isInstance( type ) )
			return DataType.UINT8;
		else
			return null;
	}

	/**
	 * Creates a {@link DataBlock} of matching type and copies the content of
	 * source into it.  This is a helper method with redundant parameters.
	 *
	 * @param source
	 * @param dataType
	 * @param intBlockSize
	 * @param longBlockSize
	 * @param gridPosition
	 * @return
	 */
	@SuppressWarnings( "unchecked" )
	private static final DataBlock< ? > createDataBlock(
			final RandomAccessibleInterval< ? > source,
			final DataType dataType,
			final int[] intBlockSize,
			final long[] longBlockSize,
			final long[] gridPosition )
	{
		final DataBlock< ? > dataBlock = dataType.createDataBlock( intBlockSize, gridPosition );
		switch ( dataType )
		{
		case UINT8:
			N5CellLoader.burnIn( ( RandomAccessibleInterval< UnsignedByteType > )source, ArrayImgs.unsignedBytes( ( byte[] )dataBlock.getData(), longBlockSize ) );
			break;
		case INT8:
			N5CellLoader.burnIn( ( RandomAccessibleInterval< ByteType > )source, ArrayImgs.bytes( ( byte[] )dataBlock.getData(), longBlockSize ) );
			break;
		case UINT16:
			N5CellLoader.burnIn( ( RandomAccessibleInterval< UnsignedShortType > )source, ArrayImgs.unsignedShorts( ( short[] )dataBlock.getData(), longBlockSize ) );
			break;
		case INT16:
			N5CellLoader.burnIn( ( RandomAccessibleInterval< ShortType > )source, ArrayImgs.shorts( ( short[] )dataBlock.getData(), longBlockSize ) );
			break;
		case UINT32:
			N5CellLoader.burnIn( ( RandomAccessibleInterval< UnsignedIntType > )source, ArrayImgs.unsignedInts( ( int[] )dataBlock.getData(), longBlockSize ) );
			break;
		case INT32:
			N5CellLoader.burnIn( ( RandomAccessibleInterval< IntType > )source, ArrayImgs.ints( ( int[] )dataBlock.getData(), longBlockSize ) );
			break;
		case UINT64:
			/* TODO missing factory method in ArrayImgs, replace when ImgLib2 updated */
			N5CellLoader.burnIn( ( RandomAccessibleInterval< UnsignedLongType > )source, ArrayImgs.unsignedLongs( new LongArray( ( long[] )dataBlock.getData() ), longBlockSize ) );
			break;
		case INT64:
			N5CellLoader.burnIn( ( RandomAccessibleInterval< LongType > )source, ArrayImgs.longs( ( long[] )dataBlock.getData(), longBlockSize ) );
			break;
		case FLOAT32:
			N5CellLoader.burnIn( ( RandomAccessibleInterval< FloatType > )source, ArrayImgs.floats( ( float[] )dataBlock.getData(), longBlockSize ) );
			break;
		case FLOAT64:
			N5CellLoader.burnIn( ( RandomAccessibleInterval< DoubleType > )source, ArrayImgs.doubles( ( double[] )dataBlock.getData(), longBlockSize ) );
			break;
		default:
			throw new IllegalArgumentException( "Type " + dataType.name() + " not supported!" );
		}

		return dataBlock;
	}

	public static void cropBlockDimensions(
			final long[] max,
			final long[] offset,
			final int[] blockDimensions,
			final long[] croppedBlockDimensions,
			final int[] intCroppedBlockDimensions,
			final long[] gridPosition )
	{
		for ( int d = 0; d < max.length; ++d )
		{
			croppedBlockDimensions[ d ] = Math.min( blockDimensions[ d ], max[ d ] - offset[ d ] + 1 );
			intCroppedBlockDimensions[ d ] = ( int )croppedBlockDimensions[ d ];
			gridPosition[ d ] = offset[ d ] / blockDimensions[ d ];
		}
	}

	public static final < T extends NativeType< T > > RandomAccessibleInterval< T > open(
			final N5 n5,
			final String dataset ) throws IOException
	{
		final DatasetAttributes attributes = n5.getDatasetAttributes( dataset );
		final long[] dimensions = attributes.getDimensions();
		final int[] cellSize = attributes.getBlockSize();

		final N5CellLoader< T > loader = new N5CellLoader<>( n5, dataset, cellSize );

		final CellGrid grid = new CellGrid( dimensions, cellSize );

		final CachedCellImg< T, ? > img;
		final T type;
		final Cache< Long, Cell< ? > > cache;

		switch ( attributes.getDataType() )
		{
		case INT8:
			type = ( T )new ByteType();
			cache = ( Cache )new SoftRefLoaderCache< Long, Cell< ByteArray > >()
					.withLoader( LoadedCellCacheLoader.get( grid, loader, type ) );
			img = new CachedCellImg( grid, type, cache, ArrayDataAccessFactory.get( PrimitiveType.BYTE ) );
			break;
		case UINT8:
			type = ( T )new UnsignedByteType();
			cache = ( Cache )new SoftRefLoaderCache< Long, Cell< ByteArray > >()
					.withLoader( LoadedCellCacheLoader.get( grid, loader, type ) );
			img = new CachedCellImg( grid,type, cache, ArrayDataAccessFactory.get( PrimitiveType.BYTE ) );
			break;
		case INT16:
			type = ( T )new ShortType();
			cache = ( Cache )new SoftRefLoaderCache< Long, Cell< ShortArray > >()
					.withLoader( LoadedCellCacheLoader.get( grid, loader, type ) );
			img = new CachedCellImg( grid, type, cache, ArrayDataAccessFactory.get( PrimitiveType.SHORT ) );
			break;
		case UINT16:
			type = ( T )new UnsignedShortType();
			cache = ( Cache )new SoftRefLoaderCache< Long, Cell< ShortArray > >()
					.withLoader( LoadedCellCacheLoader.get( grid, loader, type ) );
			img = new CachedCellImg( grid, type, cache, ArrayDataAccessFactory.get( PrimitiveType.SHORT ) );
			break;
		case INT32:
			type = ( T )new IntType();
			cache = ( Cache )new SoftRefLoaderCache< Long, Cell< IntArray > >()
					.withLoader( LoadedCellCacheLoader.get( grid, loader, type ) );
			img = new CachedCellImg( grid, type, cache, ArrayDataAccessFactory.get( PrimitiveType.INT ) );
			break;
		case UINT32:
			type = ( T )new UnsignedIntType();
			cache = ( Cache )new SoftRefLoaderCache< Long, Cell< IntArray > >()
					.withLoader( LoadedCellCacheLoader.get( grid, loader, type ) );
			img = new CachedCellImg( grid, type, cache, ArrayDataAccessFactory.get( PrimitiveType.INT ) );
			break;
		case INT64:
			type = ( T )new LongType();
			cache = ( Cache )new SoftRefLoaderCache< Long, Cell< LongArray > >()
					.withLoader( LoadedCellCacheLoader.get( grid, loader, type ) );
			img = new CachedCellImg( grid, type, cache, ArrayDataAccessFactory.get( PrimitiveType.LONG ) );
			break;
		case UINT64:
			type = ( T )new UnsignedLongType();
			cache = ( Cache )new SoftRefLoaderCache< Long, Cell< LongArray > >()
					.withLoader( LoadedCellCacheLoader.get( grid, loader, type ) );
			img = new CachedCellImg( grid, type, cache, ArrayDataAccessFactory.get( PrimitiveType.LONG ) );
			break;
		case FLOAT32:
			type = ( T )new FloatType();
			cache = ( Cache )new SoftRefLoaderCache< Long, Cell< FloatArray > >()
					.withLoader( LoadedCellCacheLoader.get( grid, loader, type ) );
			img = new CachedCellImg( grid, type, cache, ArrayDataAccessFactory.get( PrimitiveType.FLOAT ) );
			break;
		case FLOAT64:
			type = ( T )new DoubleType();
			cache = ( Cache )new SoftRefLoaderCache< Long, Cell< DoubleArray > >()
					.withLoader( LoadedCellCacheLoader.get( grid, loader, type ) );
			img = new CachedCellImg( grid, type, cache, ArrayDataAccessFactory.get( PrimitiveType.DOUBLE ) );
			break;
		default:
			img = null;
		}

		return img;
	}


	public static final < T extends NativeType< T > > RandomAccessibleInterval< T > openWithDiskCache(
			final N5 n5,
			final String dataset ) throws IOException
	{
		final DatasetAttributes attributes = n5.getDatasetAttributes( dataset );
		final long[] dimensions = attributes.getDimensions();
		final int[] cellSize = attributes.getBlockSize();

		final N5CellLoader< ? > loader = new N5CellLoader<>( n5, dataset, cellSize );

		final DiskCachedCellImgOptions options = DiskCachedCellImgOptions
				.options()
				.cellDimensions( cellSize )
				.dirtyAccesses( true )
				.maxCacheSize( 100 );

		final DiskCachedCellImgFactory factory = new DiskCachedCellImgFactory<>( options );
		final DiskCachedCellImg< T, ? > img;
		switch ( attributes.getDataType() )
		{
		case INT8:
			img = factory.create( dimensions, new ByteType(), loader );
			break;
		case UINT8:
			img = factory.create( dimensions, new UnsignedByteType(), loader );
			break;
		case INT16:
			img = factory.create( dimensions, new ShortType(), loader );
			break;
		case UINT16:
			img = factory.create( dimensions, new UnsignedShortType(), loader );
			break;
		case INT32:
			img = factory.create( dimensions, new IntType(), loader );
			break;
		case UINT32:
			img = factory.create( dimensions, new UnsignedIntType(), loader );
			break;
		case INT64:
			img = factory.create( dimensions, new LongType(), loader );
			break;
		case UINT64:
			img = factory.create( dimensions, new UnsignedLongType(), loader );
			break;
		case FLOAT32:
			img = factory.create( dimensions, new FloatType(), loader );
			break;
		case FLOAT64:
			img = factory.create( dimensions, new DoubleType(), loader );
			break;
		default:
			img = null;
		}

		return img;
	}

	public static final < T extends NativeType< T > > RandomAccessibleInterval< T > openInterval(
			final N5 n5,
			final String dataset,
			final Interval interval ) throws IOException
	{
		final RandomAccessibleInterval< T > source = open( n5, dataset );
		return Views.interval( source, interval );
	}

	public static final < T extends NativeType< T > > void save(
			RandomAccessibleInterval< T > source,
			final N5 n5,
			final String dataset,
			final int[] blockSize,
			final CompressionType compressionType,
			final ExecutorService exec ) throws IOException
	{
		source = Views.zeroMin( source );
		final long[] dimensions = Intervals.dimensionsAsLongArray( source );
		final DatasetAttributes attributes = new DatasetAttributes(
				dimensions,
				blockSize,
				dataType( Util.getTypeFromInterval( source ) ),
				compressionType );

		n5.createDataset( dataset, attributes );

		final int n = dimensions.length;
		final long[] max = Intervals.maxAsLongArray( source );
		final long[] offset = new long[ n ];
		final long[] gridPosition = new long[ n ];
		final int[] intCroppedBlockSize = new int[ n ];
		final long[] longCroppedBlockSize = new long[ n ];
		for ( int d = 0; d < n; )
		{
			cropBlockDimensions( max, offset, blockSize, longCroppedBlockSize, intCroppedBlockSize, gridPosition );
			final RandomAccessibleInterval< T > sourceBlock = Views.offsetInterval( source, offset, longCroppedBlockSize );
			final DataBlock< ? > dataBlock = createDataBlock(
					sourceBlock,
					attributes.getDataType(),
					intCroppedBlockSize,
					longCroppedBlockSize,
					gridPosition );

			n5.writeBlock(dataset, attributes, dataBlock);

			for ( d = 0; d < n; ++d )
			{
				offset[ d ] += blockSize[ d ];
				if ( offset[ d ] <= max[ d ] )
					break;
				else
					offset[ d ] = 0;
			}
		}
	}
}

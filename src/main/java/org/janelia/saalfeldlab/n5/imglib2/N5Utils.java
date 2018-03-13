/**
 *
 */
package org.janelia.saalfeldlab.n5.imglib2;

import static net.imglib2.cache.img.AccessFlags.VOLATILE;
import static net.imglib2.cache.img.PrimitiveType.BYTE;
import static net.imglib2.cache.img.PrimitiveType.DOUBLE;
import static net.imglib2.cache.img.PrimitiveType.FLOAT;
import static net.imglib2.cache.img.PrimitiveType.INT;
import static net.imglib2.cache.img.PrimitiveType.LONG;
import static net.imglib2.cache.img.PrimitiveType.SHORT;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Consumer;

import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5Writer;

import net.imglib2.Interval;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.cache.Cache;
import net.imglib2.cache.img.ArrayDataAccessFactory;
import net.imglib2.cache.img.CachedCellImg;
import net.imglib2.cache.img.DiskCachedCellImg;
import net.imglib2.cache.img.DiskCachedCellImgFactory;
import net.imglib2.cache.img.DiskCachedCellImgOptions;
import net.imglib2.cache.img.LoadedCellCacheLoader;
import net.imglib2.cache.ref.SoftRefLoaderCache;
import net.imglib2.img.Img;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.basictypeaccess.array.ByteArray;
import net.imglib2.img.basictypeaccess.array.DoubleArray;
import net.imglib2.img.basictypeaccess.array.FloatArray;
import net.imglib2.img.basictypeaccess.array.IntArray;
import net.imglib2.img.basictypeaccess.array.LongArray;
import net.imglib2.img.basictypeaccess.array.ShortArray;
import net.imglib2.img.basictypeaccess.volatiles.VolatileAccess;
import net.imglib2.img.basictypeaccess.volatiles.array.VolatileByteArray;
import net.imglib2.img.basictypeaccess.volatiles.array.VolatileDoubleArray;
import net.imglib2.img.basictypeaccess.volatiles.array.VolatileFloatArray;
import net.imglib2.img.basictypeaccess.volatiles.array.VolatileIntArray;
import net.imglib2.img.basictypeaccess.volatiles.array.VolatileLongArray;
import net.imglib2.img.basictypeaccess.volatiles.array.VolatileShortArray;
import net.imglib2.img.cell.Cell;
import net.imglib2.img.cell.CellGrid;
import net.imglib2.img.cell.LazyCellImg;
import net.imglib2.type.NativeType;
import net.imglib2.type.Type;
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

	/**
	 * Creates a {@link DataBlock} of matching type and copies the content of
	 * source into it.  This is a helper method with redundant parameters.
	 *
	 * @param source
	 * @param dataType
	 * @param intBlockSize
	 * @param longBlockSize
	 * @param gridPosition
	 * @param defaultValue
	 * @return
	 */
	@SuppressWarnings( "unchecked" )
	private static final < T extends Type< T > > DataBlock< ? > createNonEmptyDataBlock(
			final RandomAccessibleInterval< ? > source,
			final DataType dataType,
			final int[] intBlockSize,
			final long[] longBlockSize,
			final long[] gridPosition,
			final T defaultValue )
	{
		final DataBlock< ? > dataBlock = dataType.createDataBlock( intBlockSize, gridPosition );
		final boolean isEmpty;
		switch ( dataType )
		{
		case UINT8:
			isEmpty = N5CellLoader.burnInTestAllEqual(
					( RandomAccessibleInterval< UnsignedByteType > )source,
					ArrayImgs.unsignedBytes( ( byte[] )dataBlock.getData(), longBlockSize ),
					( UnsignedByteType )defaultValue );
			break;
		case INT8:
			isEmpty = N5CellLoader.burnInTestAllEqual(
					( RandomAccessibleInterval< ByteType > )source,
					ArrayImgs.bytes( ( byte[] )dataBlock.getData(), longBlockSize ),
					( ByteType )defaultValue );
			break;
		case UINT16:
			isEmpty = N5CellLoader.burnInTestAllEqual(
					( RandomAccessibleInterval< UnsignedShortType > )source,
					ArrayImgs.unsignedShorts( ( short[] )dataBlock.getData(), longBlockSize ),
					( UnsignedShortType )defaultValue );
			break;
		case INT16:
			isEmpty = N5CellLoader.burnInTestAllEqual(
					( RandomAccessibleInterval< ShortType > )source,
					ArrayImgs.shorts( ( short[] )dataBlock.getData(), longBlockSize ),
					( ShortType )defaultValue );
			break;
		case UINT32:
			isEmpty = N5CellLoader.burnInTestAllEqual(
					( RandomAccessibleInterval< UnsignedIntType > )source,
					ArrayImgs.unsignedInts( ( int[] )dataBlock.getData(), longBlockSize ),
					( UnsignedIntType )defaultValue );
			break;
		case INT32:
			isEmpty = N5CellLoader.burnInTestAllEqual(
					( RandomAccessibleInterval< IntType > )source,
					ArrayImgs.ints( ( int[] )dataBlock.getData(), longBlockSize ),
					( IntType )defaultValue );
			break;
		case UINT64:
			/* TODO missing factory method in ArrayImgs, replace when ImgLib2 updated */
			isEmpty = N5CellLoader.burnInTestAllEqual(
					( RandomAccessibleInterval< UnsignedLongType > )source,
					ArrayImgs.unsignedLongs( new LongArray( ( long[] )dataBlock.getData() ), longBlockSize ),
					( UnsignedLongType )defaultValue );
			break;
		case INT64:
			isEmpty = N5CellLoader.burnInTestAllEqual(
					( RandomAccessibleInterval< LongType > )source,
					ArrayImgs.longs( ( long[] )dataBlock.getData(), longBlockSize ),
					( LongType )defaultValue );
			break;
		case FLOAT32:
			isEmpty = N5CellLoader.burnInTestAllEqual(
					( RandomAccessibleInterval< FloatType > )source,
					ArrayImgs.floats( ( float[] )dataBlock.getData(), longBlockSize ),
					( FloatType )defaultValue );
			break;
		case FLOAT64:
			isEmpty = N5CellLoader.burnInTestAllEqual(
					( RandomAccessibleInterval< DoubleType > )source,
					ArrayImgs.doubles( ( double[] )dataBlock.getData(), longBlockSize ),
					( DoubleType )defaultValue );
			break;
		default:
			throw new IllegalArgumentException( "Type " + dataType.name() + " not supported!" );
		}

		return isEmpty ? null : dataBlock;
	}

	/**
	 * Crops the dimensions of a {@link DataBlock} at a given offset to fit
	 * into and {@link Interval} of given dimensions.  Fills long and int
	 * version of cropped block size.  Also calculates the grid raster position
	 * assuming that the offset divisible by block size without remainder.
	 *
	 * @param max
	 * @param offset
	 * @param blockDimensions
	 * @param croppedBlockDimensions
	 * @param intCroppedBlockDimensions
	 * @param gridPosition
	 */
	private static void cropBlockDimensions(
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

	/**
	 * Crops the dimensions of a {@link DataBlock} at a given offset to fit
	 * into and {@link Interval} of given dimensions.  Fills long and int
	 * version of cropped block size.  Also calculates the grid raster position
	 * plus a grid offset assuming that the offset divisible by block size
	 * without remainder.
	 *
	 * @param max
	 * @param offset
	 * @param gridOffset
	 * @param blockDimensions
	 * @param croppedBlockDimensions
	 * @param intCroppedBlockDimensions
	 * @param gridPosition
	 */
	private static void cropBlockDimensions(
			final long[] max,
			final long[] offset,
			final long[] gridOffset,
			final int[] blockDimensions,
			final long[] croppedBlockDimensions,
			final int[] intCroppedBlockDimensions,
			final long[] gridPosition )
	{
		for ( int d = 0; d < max.length; ++d )
		{
			croppedBlockDimensions[ d ] = Math.min( blockDimensions[ d ], max[ d ] - offset[ d ] + 1 );
			intCroppedBlockDimensions[ d ] = ( int )croppedBlockDimensions[ d ];
			gridPosition[ d ] = offset[ d ] / blockDimensions[ d ] + gridOffset[ d ];
		}
	}

	/**
	 * Open an N5 dataset as a memory cached {@link LazyCellImg}.
	 *
	 * @param n5
	 * @param dataset
	 * @return
	 * @throws IOException
	 */
	public static final < T extends NativeType< T > > RandomAccessibleInterval< T > open(
			final N5Reader n5,
			final String dataset ) throws IOException
	{
		return openSparse( n5, dataset, ( Consumer< Img< T > > ) img -> {} );
	}

	/**
	 * Open an N5 dataset as a memory cached {@link LazyCellImg} using {@link VolatileAccess}.
	 *
	 * @param n5
	 * @param dataset
	 * @return
	 * @throws IOException
	 */
	public static final < T extends NativeType< T > > RandomAccessibleInterval< T > openVolatile(
			final N5Reader n5,
			final String dataset ) throws IOException
	{
		return openSparseVolatile( n5, dataset, ( Consumer< Img< T > > ) img -> {} );
	}

	/**
	 * Open an N5 dataset as a disk-cached {@link LazyCellImg}. Note that this
	 * requires that all parts of the the N5 dataset that will be accessed fit
	 * into /tmp.
	 *
	 * @param n5
	 * @param dataset
	 * @return
	 * @throws IOException
	 */
	public static final < T extends NativeType< T > > RandomAccessibleInterval< T > openWithDiskCache(
			final N5Reader n5,
			final String dataset ) throws IOException
	{
		return openSparseWithDiskCache( n5, dataset, ( Consumer< Img< T > > ) img -> {} );
	}

	/**
	 * Open an N5 dataset as a memory cached {@link LazyCellImg}.
	 *
	 * @param n5
	 * @param dataset
	 * @param defaultValue
	 * @return
	 * @throws IOException
	 */
	public static final < T extends NativeType< T > > RandomAccessibleInterval< T > openSparse(
			final N5Reader n5,
			final String dataset,
			final T defaultValue ) throws IOException
	{
		return openSparse( n5, dataset, N5CellLoader.setToDefaultValue( defaultValue ) );
	}

	/**
	 * Open an N5 dataset as a memory cached {@link LazyCellImg} using {@link VolatileAccess}.
	 *
	 * @param n5
	 * @param dataset
	 * @param defaultValue
	 * @return
	 * @throws IOException
	 */
	public static final < T extends NativeType< T > > RandomAccessibleInterval< T > openSparseVolatile(
			final N5Reader n5,
			final String dataset,
			final T defaultValue ) throws IOException
	{
		return openSparseVolatile( n5, dataset, N5CellLoader.setToDefaultValue( defaultValue ) );
	}

	/**
	 * Open an N5 dataset as a disk-cached {@link LazyCellImg}. Note that this
	 * requires that all parts of the the N5 dataset that will be accessed fit
	 * into /tmp.
	 *
	 * @param n5
	 * @param dataset
	 * @param defaultValue
	 * @return
	 * @throws IOException
	 */
	public static final < T extends NativeType< T > > RandomAccessibleInterval< T > openSparseWithDiskCache(
			final N5Reader n5,
			final String dataset,
			final T defaultValue ) throws IOException
	{
		return openSparseWithDiskCache( n5, dataset, N5CellLoader.setToDefaultValue( defaultValue ) );
	}

	/**
	 * Open an N5 dataset as a memory cached {@link LazyCellImg}.
	 *
	 * @param n5
	 * @param dataset
	 * @param blockNotFoundHandler
	 * @return
	 * @throws IOException
	 */
	@SuppressWarnings( { "unchecked", "rawtypes" } )
	public static final < T extends NativeType< T > > RandomAccessibleInterval< T > openSparse(
			final N5Reader n5,
			final String dataset,
			final Consumer< Img< T > > blockNotFoundHandler ) throws IOException
	{
		final DatasetAttributes attributes = n5.getDatasetAttributes( dataset );
		final long[] dimensions = attributes.getDimensions();
		final int[] blockSize = attributes.getBlockSize();

		final N5CellLoader< T > loader = new N5CellLoader<>( n5, dataset, blockSize, blockNotFoundHandler );

		final CellGrid grid = new CellGrid( dimensions, blockSize );

		final CachedCellImg< T, ? > img;
		final T type;
		final Cache< Long, Cell< ? > > cache;

		switch ( attributes.getDataType() )
		{
		case INT8:
			type = ( T )new ByteType();
			cache = ( Cache )new SoftRefLoaderCache< Long, Cell< ByteArray > >()
					.withLoader( LoadedCellCacheLoader.get( grid, loader, type ) );
			img = new CachedCellImg( grid, type, cache, ArrayDataAccessFactory.get( BYTE ) );
			break;
		case UINT8:
			type = ( T )new UnsignedByteType();
			cache = ( Cache )new SoftRefLoaderCache< Long, Cell< ByteArray > >()
					.withLoader( LoadedCellCacheLoader.get( grid, loader, type ) );
			img = new CachedCellImg( grid, type, cache, ArrayDataAccessFactory.get( BYTE ) );
			break;
		case INT16:
			type = ( T )new ShortType();
			cache = ( Cache )new SoftRefLoaderCache< Long, Cell< ShortArray > >()
					.withLoader( LoadedCellCacheLoader.get( grid, loader, type ) );
			img = new CachedCellImg( grid, type, cache, ArrayDataAccessFactory.get( SHORT ) );
			break;
		case UINT16:
			type = ( T )new UnsignedShortType();
			cache = ( Cache )new SoftRefLoaderCache< Long, Cell< ShortArray > >()
					.withLoader( LoadedCellCacheLoader.get( grid, loader, type ) );
			img = new CachedCellImg( grid, type, cache, ArrayDataAccessFactory.get( SHORT ) );
			break;
		case INT32:
			type = ( T )new IntType();
			cache = ( Cache )new SoftRefLoaderCache< Long, Cell< IntArray > >()
					.withLoader( LoadedCellCacheLoader.get( grid, loader, type ) );
			img = new CachedCellImg( grid, type, cache, ArrayDataAccessFactory.get( INT ) );
			break;
		case UINT32:
			type = ( T )new UnsignedIntType();
			cache = ( Cache )new SoftRefLoaderCache< Long, Cell< IntArray > >()
					.withLoader( LoadedCellCacheLoader.get( grid, loader, type ) );
			img = new CachedCellImg( grid, type, cache, ArrayDataAccessFactory.get( INT ) );
			break;
		case INT64:
			type = ( T )new LongType();
			cache = ( Cache )new SoftRefLoaderCache< Long, Cell< LongArray > >()
					.withLoader( LoadedCellCacheLoader.get( grid, loader, type ) );
			img = new CachedCellImg( grid, type, cache, ArrayDataAccessFactory.get( LONG ) );
			break;
		case UINT64:
			type = ( T )new UnsignedLongType();
			cache = ( Cache )new SoftRefLoaderCache< Long, Cell< LongArray > >()
					.withLoader( LoadedCellCacheLoader.get( grid, loader, type ) );
			img = new CachedCellImg( grid, type, cache, ArrayDataAccessFactory.get( LONG ) );
			break;
		case FLOAT32:
			type = ( T )new FloatType();
			cache = ( Cache )new SoftRefLoaderCache< Long, Cell< FloatArray > >()
					.withLoader( LoadedCellCacheLoader.get( grid, loader, type ) );
			img = new CachedCellImg( grid, type, cache, ArrayDataAccessFactory.get( FLOAT ) );
			break;
		case FLOAT64:
			type = ( T )new DoubleType();
			cache = ( Cache )new SoftRefLoaderCache< Long, Cell< DoubleArray > >()
					.withLoader( LoadedCellCacheLoader.get( grid, loader, type ) );
			img = new CachedCellImg( grid, type, cache, ArrayDataAccessFactory.get( DOUBLE ) );
			break;
		default:
			img = null;
		}

		return img;
	}

	/**
	 * Open an N5 dataset as a memory cached {@link LazyCellImg} using {@link VolatileAccess}.
	 *
	 * @param n5
	 * @param dataset
	 * @param blockNotFoundHandler
	 * @return
	 * @throws IOException
	 */
	@SuppressWarnings( { "unchecked", "rawtypes" } )
	public static final < T extends NativeType< T > > RandomAccessibleInterval< T > openSparseVolatile(
			final N5Reader n5,
			final String dataset,
			final Consumer< Img< T > > blockNotFoundHandler ) throws IOException
	{
		final DatasetAttributes attributes = n5.getDatasetAttributes( dataset );
		final long[] dimensions = attributes.getDimensions();
		final int[] blockSize = attributes.getBlockSize();

		final N5CellLoader< T > loader = new N5CellLoader<>( n5, dataset, blockSize, blockNotFoundHandler );

		final CellGrid grid = new CellGrid( dimensions, blockSize );

		final CachedCellImg< T, ? > img;
		final T type;
		final Cache< Long, Cell< ? > > cache;

		switch ( attributes.getDataType() )
		{
		case INT8:
			type = ( T )new ByteType();
			cache = ( Cache )new SoftRefLoaderCache< Long, Cell< VolatileByteArray > >()
					.withLoader( LoadedCellCacheLoader.get( grid, loader, type, VOLATILE ) );
			img = new CachedCellImg( grid, type, cache, ArrayDataAccessFactory.get( BYTE, VOLATILE ) );
			break;
		case UINT8:
			type = ( T )new UnsignedByteType();
			cache = ( Cache )new SoftRefLoaderCache< Long, Cell< VolatileByteArray > >()
					.withLoader( LoadedCellCacheLoader.get( grid, loader, type, VOLATILE ) );
			img = new CachedCellImg( grid, type, cache, ArrayDataAccessFactory.get( BYTE, VOLATILE ) );
			break;
		case INT16:
			type = ( T )new ShortType();
			cache = ( Cache )new SoftRefLoaderCache< Long, Cell< VolatileShortArray > >()
					.withLoader( LoadedCellCacheLoader.get( grid, loader, type, VOLATILE ) );
			img = new CachedCellImg( grid, type, cache, ArrayDataAccessFactory.get( SHORT, VOLATILE ) );
			break;
		case UINT16:
			type = ( T )new UnsignedShortType();
			cache = ( Cache )new SoftRefLoaderCache< Long, Cell< VolatileShortArray > >()
					.withLoader( LoadedCellCacheLoader.get( grid, loader, type, VOLATILE ) );
			img = new CachedCellImg( grid, type, cache, ArrayDataAccessFactory.get( SHORT, VOLATILE ) );
			break;
		case INT32:
			type = ( T )new IntType();
			cache = ( Cache )new SoftRefLoaderCache< Long, Cell< VolatileIntArray > >()
					.withLoader( LoadedCellCacheLoader.get( grid, loader, type, VOLATILE ) );
			img = new CachedCellImg( grid, type, cache, ArrayDataAccessFactory.get( INT, VOLATILE ) );
			break;
		case UINT32:
			type = ( T )new UnsignedIntType();
			cache = ( Cache )new SoftRefLoaderCache< Long, Cell< VolatileIntArray > >()
					.withLoader( LoadedCellCacheLoader.get( grid, loader, type, VOLATILE ) );
			img = new CachedCellImg( grid, type, cache, ArrayDataAccessFactory.get( INT, VOLATILE ) );
			break;
		case INT64:
			type = ( T )new LongType();
			cache = ( Cache )new SoftRefLoaderCache< Long, Cell< VolatileLongArray > >()
					.withLoader( LoadedCellCacheLoader.get( grid, loader, type, VOLATILE ) );
			img = new CachedCellImg( grid, type, cache, ArrayDataAccessFactory.get( LONG, VOLATILE ) );
			break;
		case UINT64:
			type = ( T )new UnsignedLongType();
			cache = ( Cache )new SoftRefLoaderCache< Long, Cell< VolatileLongArray > >()
					.withLoader( LoadedCellCacheLoader.get( grid, loader, type, VOLATILE ) );
			img = new CachedCellImg( grid, type, cache, ArrayDataAccessFactory.get( LONG, VOLATILE ) );
			break;
		case FLOAT32:
			type = ( T )new FloatType();
			cache = ( Cache )new SoftRefLoaderCache< Long, Cell< VolatileFloatArray > >()
					.withLoader( LoadedCellCacheLoader.get( grid, loader, type, VOLATILE ) );
			img = new CachedCellImg( grid, type, cache, ArrayDataAccessFactory.get( FLOAT, VOLATILE ) );
			break;
		case FLOAT64:
			type = ( T )new DoubleType();
			cache = ( Cache )new SoftRefLoaderCache< Long, Cell< VolatileDoubleArray > >()
					.withLoader( LoadedCellCacheLoader.get( grid, loader, type, VOLATILE ) );
			img = new CachedCellImg( grid, type, cache, ArrayDataAccessFactory.get( DOUBLE, VOLATILE ) );
			break;
		default:
			img = null;
		}

		return img;
	}

	/**
	 * Open an N5 dataset as a disk-cached {@link LazyCellImg}. Note that this
	 * requires that all parts of the the N5 dataset that will be accessed fit
	 * into /tmp.
	 *
	 * @param n5
	 * @param dataset
	 * @param blockNotFoundHandler
	 * @return
	 * @throws IOException
	 */
	@SuppressWarnings( { "unchecked", "rawtypes" } )
	public static final < T extends NativeType< T > > RandomAccessibleInterval< T > openSparseWithDiskCache(
			final N5Reader n5,
			final String dataset,
			final Consumer< Img< T > > blockNotFoundHandler ) throws IOException
	{
		final DatasetAttributes attributes = n5.getDatasetAttributes( dataset );
		final long[] dimensions = attributes.getDimensions();
		final int[] blockSize = attributes.getBlockSize();

		final N5CellLoader< ? > loader = new N5CellLoader<>( n5, dataset, blockSize, blockNotFoundHandler );

		final DiskCachedCellImgOptions options = DiskCachedCellImgOptions
				.options()
				.cellDimensions( blockSize )
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

	/**
	 * Save a {@link RandomAccessibleInterval} as an N5 dataset.
	 *
	 * @param source
	 * @param n5
	 * @param dataset
	 * @param attributes
	 * @param gridOffset
	 * @throws IOException
	 */
	public static final < T extends NativeType< T > > void saveBlock(
			RandomAccessibleInterval< T > source,
			final N5Writer n5,
			final String dataset,
			final DatasetAttributes attributes,
			final long[] gridOffset ) throws IOException
	{
		source = Views.zeroMin( source );
		final long[] dimensions = Intervals.dimensionsAsLongArray( source );

		final int n = dimensions.length;
		final long[] max = Intervals.maxAsLongArray( source );
		final long[] offset = new long[ n ];
		final long[] gridPosition = new long[ n ];
		final int[] blockSize = attributes.getBlockSize();
		final int[] intCroppedBlockSize = new int[ n ];
		final long[] longCroppedBlockSize = new long[ n ];
		for ( int d = 0; d < n; )
		{
			cropBlockDimensions( max, offset, gridOffset, blockSize, longCroppedBlockSize, intCroppedBlockSize, gridPosition );
			final RandomAccessibleInterval< T > sourceBlock = Views.offsetInterval( source, offset, longCroppedBlockSize );
			final DataBlock< ? > dataBlock = createDataBlock(
					sourceBlock,
					attributes.getDataType(),
					intCroppedBlockSize,
					longCroppedBlockSize,
					gridPosition );

			n5.writeBlock( dataset, attributes, dataBlock );

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

	/**
	 * Save a {@link RandomAccessibleInterval} as an N5 dataset.
	 *
	 * @param source
	 * @param n5
	 * @param dataset
	 * @param gridOffset
	 * @throws IOException
	 */
	public static final < T extends NativeType< T > > void saveBlock(
			final RandomAccessibleInterval< T > source,
			final N5Writer n5,
			final String dataset,
			final long[] gridOffset ) throws IOException
	{
		final DatasetAttributes attributes = n5.getDatasetAttributes( dataset );
		if ( attributes != null )
		{
			saveBlock( source, n5, dataset, attributes, gridOffset );
		}
		else
		{
			throw new IOException( "Dataset " + dataset + " does not exist." );
		}
	}

	/**
	 * Save a {@link RandomAccessibleInterval} as an N5 dataset,
	 * multi-threaded.
	 *
	 * @param source
	 * @param n5
	 * @param dataset
	 * @param blockSize
	 * @param compressionType
	 * @param exec
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	public static final < T extends NativeType< T > > void saveBlock(
			final RandomAccessibleInterval< T > source,
			final N5Writer n5,
			final String dataset,
			final long[] gridOffset,
			final ExecutorService exec ) throws IOException, InterruptedException, ExecutionException
	{
		final RandomAccessibleInterval< T > zeroMinSource = Views.zeroMin( source );
		final long[] dimensions = Intervals.dimensionsAsLongArray( zeroMinSource );
		final DatasetAttributes attributes = n5.getDatasetAttributes( dataset );
		if ( attributes != null )
		{
			final int n = dimensions.length;
			final long[] max = Intervals.maxAsLongArray( zeroMinSource );
			final long[] offset = new long[ n ];
			final int[] blockSize = attributes.getBlockSize();

			final ArrayList< Future< ? > > futures = new ArrayList<>();
			for ( int d = 0; d < n; )
			{
				final long[] fOffset = offset.clone();

				futures.add(
						exec.submit(
								() -> {

									final long[] gridPosition = new long[ n ];
									final int[] intCroppedBlockSize = new int[ n ];
									final long[] longCroppedBlockSize = new long[ n ];

									cropBlockDimensions( max, fOffset, gridOffset, blockSize, longCroppedBlockSize, intCroppedBlockSize, gridPosition );

									final RandomAccessibleInterval< T > sourceBlock = Views.offsetInterval( zeroMinSource, fOffset, longCroppedBlockSize );
									final DataBlock< ? > dataBlock = createDataBlock(
											sourceBlock,
											attributes.getDataType(),
											intCroppedBlockSize,
											longCroppedBlockSize,
											gridPosition );

									try
									{
										n5.writeBlock( dataset, attributes, dataBlock );
									}
									catch ( final IOException e )
									{
										e.printStackTrace();
									}
								} ) );

				for ( d = 0; d < n; ++d )
				{
					offset[ d ] += blockSize[ d ];
					if ( offset[ d ] <= max[ d ] )
						break;
					else
						offset[ d ] = 0;
				}
			}
			for ( final Future< ? > f : futures )
				f.get();
		}
		else
		{
			throw new IOException( "Dataset " + dataset + " does not exist." );
		}
	}

	/**
	 * Save a {@link RandomAccessibleInterval} into an N5 dataset at a given
	 * offset.  The offset is given in {@link DataBlock} grid coordinates and
	 * the source is assumed to align with the {@link DataBlock} grid of the
	 * dataset.  Only {@link DataBlock DataBlocks} that contain values other
	 * than a given default value are stored.
	 *
	 * @param source
	 * @param n5
	 * @param dataset
	 * @param attributes
	 * @param gridOffset
	 * @param defaultValue
	 * @throws IOException
	 */
	public static final < T extends NativeType< T > > void saveNonEmptyBlock(
			RandomAccessibleInterval< T > source,
			final N5Writer n5,
			final String dataset,
			final DatasetAttributes attributes,
			final long[] gridOffset,
			final T defaultValue ) throws IOException
	{
		source = Views.zeroMin( source );
		final long[] dimensions = Intervals.dimensionsAsLongArray( source );

		final int n = dimensions.length;
		final long[] max = Intervals.maxAsLongArray( source );
		final long[] offset = new long[ n ];
		final long[] gridPosition = new long[ n ];
		final int[] blockSize = attributes.getBlockSize();
		final int[] intCroppedBlockSize = new int[ n ];
		final long[] longCroppedBlockSize = new long[ n ];
		for ( int d = 0; d < n; )
		{
			cropBlockDimensions( max, offset, gridOffset, blockSize, longCroppedBlockSize, intCroppedBlockSize, gridPosition );
			final RandomAccessibleInterval< T > sourceBlock = Views.offsetInterval( source, offset, longCroppedBlockSize );
			final DataBlock< ? > dataBlock = createNonEmptyDataBlock(
					sourceBlock,
					attributes.getDataType(),
					intCroppedBlockSize,
					longCroppedBlockSize,
					gridPosition,
					defaultValue );

			if ( dataBlock != null )
				n5.writeBlock( dataset, attributes, dataBlock );

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

	/**
	 * Save a {@link RandomAccessibleInterval} into an N5 dataset at a given
	 * offset.  The offset is given in {@link DataBlock} grid coordinates and
	 * the source is assumed to align with the {@link DataBlock} grid of the
	 * dataset.  Only {@link DataBlock DataBlocks} that contain values other
	 * than a given default value are stored.
	 *
	 * @param source
	 * @param n5
	 * @param dataset
	 * @param gridOffset
	 * @param defaultValue
	 * @throws IOException
	 */
	public static final < T extends NativeType< T > > void saveNonEmptyBlock(
			final RandomAccessibleInterval< T > source,
			final N5Writer n5,
			final String dataset,
			final long[] gridOffset,
			final T defaultValue ) throws IOException
	{
		final DatasetAttributes attributes = n5.getDatasetAttributes( dataset );
		if ( attributes != null )
		{
			saveNonEmptyBlock( source, n5, dataset, attributes, gridOffset, defaultValue );
		}
		else
		{
			throw new IOException( "Dataset " + dataset + " does not exist." );
		}
	}

	/**
	 * Save a {@link RandomAccessibleInterval} as an N5 dataset.
	 *
	 * @param source
	 * @param n5
	 * @param dataset
	 * @param blockSize
	 * @param compressionType
	 * @throws IOException
	 */
	public static final < T extends NativeType< T > > void save(
			RandomAccessibleInterval< T > source,
			final N5Writer n5,
			final String dataset,
			final int[] blockSize,
			final Compression compression) throws IOException
	{
		source = Views.zeroMin( source );
		final long[] dimensions = Intervals.dimensionsAsLongArray( source );
		final DatasetAttributes attributes = new DatasetAttributes(
				dimensions,
				blockSize,
				dataType( Util.getTypeFromInterval( source ) ),
				compression );

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

			n5.writeBlock( dataset, attributes, dataBlock );

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

	/**
	 * Save a {@link RandomAccessibleInterval} as an N5 dataset,
	 * multi-threaded.
	 *
	 * @param source
	 * @param n5
	 * @param dataset
	 * @param blockSize
	 * @param compressionType
	 * @param exec
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	public static final < T extends NativeType< T > > void save(
			final RandomAccessibleInterval< T > source,
			final N5Writer n5,
			final String dataset,
			final int[] blockSize,
			final Compression compression,
			final ExecutorService exec ) throws IOException, InterruptedException, ExecutionException
	{
		final RandomAccessibleInterval< T > zeroMinSource = Views.zeroMin( source );
		final long[] dimensions = Intervals.dimensionsAsLongArray( zeroMinSource );
		final DatasetAttributes attributes = new DatasetAttributes(
				dimensions,
				blockSize,
				dataType( Util.getTypeFromInterval( zeroMinSource ) ),
				compression );

		n5.createDataset( dataset, attributes );

		final int n = dimensions.length;
		final long[] max = Intervals.maxAsLongArray( zeroMinSource );
		final long[] offset = new long[ n ];

		final ArrayList< Future< ? > > futures = new ArrayList<>();
		for ( int d = 0; d < n; )
		{
			final long[] fOffset = offset.clone();

			futures.add(
					exec.submit(
							() -> {

								final long[] gridPosition = new long[ n ];
								final int[] intCroppedBlockSize = new int[ n ];
								final long[] longCroppedBlockSize = new long[ n ];

								cropBlockDimensions( max, fOffset, blockSize, longCroppedBlockSize, intCroppedBlockSize, gridPosition );

								final RandomAccessibleInterval< T > sourceBlock = Views.offsetInterval( zeroMinSource, fOffset, longCroppedBlockSize );
								final DataBlock< ? > dataBlock = createDataBlock(
										sourceBlock,
										attributes.getDataType(),
										intCroppedBlockSize,
										longCroppedBlockSize,
										gridPosition );

								try
								{
									n5.writeBlock( dataset, attributes, dataBlock );
								}
								catch ( final IOException e )
								{
									e.printStackTrace();
								}
							} ) );

			for ( d = 0; d < n; ++d )
			{
				offset[ d ] += blockSize[ d ];
				if ( offset[ d ] <= max[ d ] )
					break;
				else
					offset[ d ] = 0;
			}
		}
		for ( final Future< ? > f : futures )
			f.get();
	}
}

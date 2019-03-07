/**
 * Copyright (c) 2017-2018, Stephan Saalfeld, Philipp Hanslovsky, Igor Pisarev
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * Redistributions of source code must retain the above copyright notice, this
 *  list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package org.janelia.saalfeldlab.n5.imglib2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntFunction;

import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5Writer;

import net.imglib2.Interval;
import net.imglib2.IterableInterval;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.cache.Cache;
import net.imglib2.cache.LoaderCache;
import net.imglib2.cache.img.CachedCellImg;
import net.imglib2.cache.img.DiskCachedCellImgFactory;
import net.imglib2.cache.img.DiskCachedCellImgOptions;
import net.imglib2.cache.img.LoadedCellCacheLoader;
import net.imglib2.cache.ref.BoundedSoftRefLoaderCache;
import net.imglib2.cache.ref.SoftRefLoaderCache;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.basictypeaccess.AccessFlags;
import net.imglib2.img.basictypeaccess.ArrayDataAccessFactory;
import net.imglib2.img.basictypeaccess.array.ArrayDataAccess;
import net.imglib2.img.basictypeaccess.volatiles.VolatileAccess;
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
import net.imglib2.util.Pair;
import net.imglib2.util.Util;
import net.imglib2.util.ValuePair;
import net.imglib2.view.Views;

/**
 * Static utility methods to open N5 datasets as ImgLib2
 * {@link RandomAccessibleInterval RandomAccessibleIntervals} and to save
 * ImgLib2 {@link RandomAccessibleInterval RandomAccessibleIntervals} as
 * [sparse] N5 datasets.
 *
 * @author Stephan Saalfeld &lt;saalfelds@janelia.hhmi.org&gt;
 * @author Philipp Hanslovsky &lt;hanslovskyp@janelia.hhmi.org&gt;
 */
public class N5Utils {

	private N5Utils() {}

	public static final <T extends NativeType<T>> DataType dataType(final T type) {

		if (DoubleType.class.isInstance(type))
			return DataType.FLOAT64;
		if (FloatType.class.isInstance(type))
			return DataType.FLOAT32;
		if (LongType.class.isInstance(type))
			return DataType.INT64;
		if (UnsignedLongType.class.isInstance(type))
			return DataType.UINT64;
		if (IntType.class.isInstance(type))
			return DataType.INT32;
		if (UnsignedIntType.class.isInstance(type))
			return DataType.UINT32;
		if (ShortType.class.isInstance(type))
			return DataType.INT16;
		if (UnsignedShortType.class.isInstance(type))
			return DataType.UINT16;
		if (ByteType.class.isInstance(type))
			return DataType.INT8;
		if (UnsignedByteType.class.isInstance(type))
			return DataType.UINT8;
		else
			return null;
	}

	@SuppressWarnings("unchecked")
	public static final <T extends NativeType<T>> T type(final DataType dataType) {

		switch (dataType) {
			case INT8:
				return (T) new ByteType();
			case UINT8:
				return (T) new UnsignedByteType();
			case INT16:
				return (T) new ShortType();
			case UINT16:
				return (T) new UnsignedShortType();
			case INT32:
				return (T) new IntType();
			case UINT32:
				return (T) new UnsignedIntType();
			case INT64:
				return (T) new LongType();
			case UINT64:
				return (T) new UnsignedLongType();
			case FLOAT32:
				return (T) new FloatType();
			case FLOAT64:
				return (T) new DoubleType();
			default:
				return null;
		}
	}

	/**
	 * Creates a {@link DataBlock} of matching type and copies the content of
	 * source into it. This is a helper method with redundant parameters.
	 *
	 * @param source
	 * @param dataType
	 * @param intBlockSize
	 * @param longBlockSize
	 * @param gridPosition
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private static final DataBlock<?> createDataBlock(
			final RandomAccessibleInterval<?> source,
			final DataType dataType,
			final int[] intBlockSize,
			final long[] longBlockSize,
			final long[] gridPosition) {

		final DataBlock<?> dataBlock = dataType.createDataBlock(intBlockSize, gridPosition);
		switch (dataType) {
		case UINT8:
			N5CellLoader.burnIn(
					(RandomAccessibleInterval<UnsignedByteType>)source,
					ArrayImgs.unsignedBytes((byte[])dataBlock.getData(), longBlockSize));
			break;
		case INT8:
			N5CellLoader.burnIn(
					(RandomAccessibleInterval<ByteType>)source,
					ArrayImgs.bytes((byte[])dataBlock.getData(), longBlockSize));
			break;
		case UINT16:
			N5CellLoader.burnIn(
					(RandomAccessibleInterval<UnsignedShortType>)source,
					ArrayImgs.unsignedShorts((short[])dataBlock.getData(), longBlockSize));
			break;
		case INT16:
			N5CellLoader.burnIn(
					(RandomAccessibleInterval<ShortType>)source,
					ArrayImgs.shorts((short[])dataBlock.getData(), longBlockSize));
			break;
		case UINT32:
			N5CellLoader.burnIn(
					(RandomAccessibleInterval<UnsignedIntType>)source,
					ArrayImgs.unsignedInts((int[])dataBlock.getData(), longBlockSize));
			break;
		case INT32:
			N5CellLoader.burnIn(
					(RandomAccessibleInterval<IntType>)source,
					ArrayImgs.ints((int[])dataBlock.getData(), longBlockSize));
			break;
		case UINT64:
			N5CellLoader.burnIn(
					(RandomAccessibleInterval<UnsignedLongType>)source,
					ArrayImgs.unsignedLongs((long[])dataBlock.getData(), longBlockSize));
			break;
		case INT64:
			N5CellLoader.burnIn(
					(RandomAccessibleInterval<LongType>)source,
					ArrayImgs.longs((long[])dataBlock.getData(), longBlockSize));
			break;
		case FLOAT32:
			N5CellLoader.burnIn(
					(RandomAccessibleInterval<FloatType>)source,
					ArrayImgs.floats((float[])dataBlock.getData(), longBlockSize));
			break;
		case FLOAT64:
			N5CellLoader.burnIn(
					(RandomAccessibleInterval<DoubleType>)source,
					ArrayImgs.doubles((double[])dataBlock.getData(), longBlockSize));
			break;
		default:
			throw new IllegalArgumentException("Type " + dataType.name() + " not supported!");
		}

		return dataBlock;
	}

	/**
	 * Creates a {@link DataBlock} of matching type and copies the content of
	 * source into it. This is a helper method with redundant parameters.
	 *
	 * @param source
	 * @param dataType
	 * @param intBlockSize
	 * @param longBlockSize
	 * @param gridPosition
	 * @param defaultValue
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private static final <T extends Type<T>> DataBlock<?> createNonEmptyDataBlock(
			final RandomAccessibleInterval<?> source,
			final DataType dataType,
			final int[] intBlockSize,
			final long[] longBlockSize,
			final long[] gridPosition,
			final T defaultValue) {

		final DataBlock<?> dataBlock = dataType.createDataBlock(intBlockSize, gridPosition);
		final boolean isEmpty;
		switch (dataType) {
		case UINT8:
			isEmpty = N5CellLoader.burnInTestAllEqual(
					(RandomAccessibleInterval<UnsignedByteType>)source,
					ArrayImgs.unsignedBytes((byte[])dataBlock.getData(), longBlockSize),
					(UnsignedByteType)defaultValue);
			break;
		case INT8:
			isEmpty = N5CellLoader.burnInTestAllEqual(
					(RandomAccessibleInterval<ByteType>)source,
					ArrayImgs.bytes((byte[])dataBlock.getData(), longBlockSize),
					(ByteType)defaultValue);
			break;
		case UINT16:
			isEmpty = N5CellLoader.burnInTestAllEqual(
					(RandomAccessibleInterval<UnsignedShortType>)source,
					ArrayImgs.unsignedShorts((short[])dataBlock.getData(), longBlockSize),
					(UnsignedShortType)defaultValue);
			break;
		case INT16:
			isEmpty = N5CellLoader.burnInTestAllEqual(
					(RandomAccessibleInterval<ShortType>)source,
					ArrayImgs.shorts((short[])dataBlock.getData(), longBlockSize),
					(ShortType)defaultValue);
			break;
		case UINT32:
			isEmpty = N5CellLoader.burnInTestAllEqual(
					(RandomAccessibleInterval<UnsignedIntType>)source,
					ArrayImgs.unsignedInts((int[])dataBlock.getData(), longBlockSize),
					(UnsignedIntType)defaultValue);
			break;
		case INT32:
			isEmpty = N5CellLoader.burnInTestAllEqual(
					(RandomAccessibleInterval<IntType>)source,
					ArrayImgs.ints((int[])dataBlock.getData(), longBlockSize),
					(IntType)defaultValue);
			break;
		case UINT64:
			isEmpty = N5CellLoader.burnInTestAllEqual(
					(RandomAccessibleInterval<UnsignedLongType>)source,
					ArrayImgs.unsignedLongs((long[])dataBlock.getData(), longBlockSize),
					(UnsignedLongType)defaultValue);
			break;
		case INT64:
			isEmpty = N5CellLoader.burnInTestAllEqual(
					(RandomAccessibleInterval<LongType>)source,
					ArrayImgs.longs((long[])dataBlock.getData(), longBlockSize),
					(LongType)defaultValue);
			break;
		case FLOAT32:
			isEmpty = N5CellLoader.burnInTestAllEqual(
					(RandomAccessibleInterval<FloatType>)source,
					ArrayImgs.floats((float[])dataBlock.getData(), longBlockSize),
					(FloatType)defaultValue);
			break;
		case FLOAT64:
			isEmpty = N5CellLoader.burnInTestAllEqual(
					(RandomAccessibleInterval<DoubleType>)source,
					ArrayImgs.doubles((double[])dataBlock.getData(), longBlockSize),
					(DoubleType)defaultValue);
			break;
		default:
			throw new IllegalArgumentException("Type " + dataType.name() + " not supported!");
		}

		return isEmpty ? null : dataBlock;
	}

	/**
	 * Crops the dimensions of a {@link DataBlock} at a given offset to fit into
	 * and {@link Interval} of given dimensions. Fills long and int version of
	 * cropped block size. Also calculates the grid raster position assuming
	 * that the offset divisible by block size without remainder.
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
			final long[] gridPosition) {

		for (int d = 0; d < max.length; ++d) {
			croppedBlockDimensions[d] = Math.min(blockDimensions[d], max[d] - offset[d] + 1);
			intCroppedBlockDimensions[d] = (int)croppedBlockDimensions[d];
			gridPosition[d] = offset[d] / blockDimensions[d];
		}
	}

	/**
	 * Crops the dimensions of a {@link DataBlock} at a given offset to fit into
	 * and {@link Interval} of given dimensions. Fills long and int version of
	 * cropped block size. Also calculates the grid raster position plus a grid
	 * offset assuming that the offset divisible by block size without
	 * remainder.
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
			final long[] gridPosition) {

		for (int d = 0; d < max.length; ++d) {
			croppedBlockDimensions[d] = Math.min(blockDimensions[d], max[d] - offset[d] + 1);
			intCroppedBlockDimensions[d] = (int)croppedBlockDimensions[d];
			gridPosition[d] = offset[d] / blockDimensions[d] + gridOffset[d];
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
	public static final <T extends NativeType<T>> RandomAccessibleInterval<T> open(
			final N5Reader n5,
			final String dataset) throws IOException {

		return open(n5, dataset, (Consumer<IterableInterval<T>>)img -> {});
	}


	/**
	 * Open an N5 dataset as a memory cached {@link LazyCellImg}.
	 *
	 * @param n5
	 * @param dataset
	 * @param maxNumCacheEntries
	 * @return
	 * @throws IOException
	 */
	public static final <T extends NativeType<T>> RandomAccessibleInterval openWithBoundedSoftRefCache(
			final N5Reader n5,
			final String dataset,
			final int maxNumCacheEntries) throws IOException
	{
		return openWithBoundedSoftRefCache(n5, dataset, (Consumer<IterableInterval<T>>)img -> {}, maxNumCacheEntries);
	}

	/**
	 * Open an N5 dataset as a memory cached {@link LazyCellImg} using
	 * {@link VolatileAccess}.
	 *
	 * @param n5
	 * @param dataset
	 * @return
	 * @throws IOException
	 */
	public static final <T extends NativeType<T>> RandomAccessibleInterval<T> openVolatile(
			final N5Reader n5,
			final String dataset) throws IOException {

		return openVolatile(n5, dataset, (Consumer<IterableInterval<T>>)img -> {});
	}


	/**
	 * Open an N5 dataset as a memory cached {@link LazyCellImg} using
	 * {@link VolatileAccess}.
	 *
	 * @param n5
	 * @param dataset
	 * @param maxNumCacheEntries
	 * @return
	 * @throws IOException
	 */
	public static final <T extends NativeType<T>> RandomAccessibleInterval openVolatileWithBoundedSoftRefCache(
			final N5Reader n5,
			final String dataset,
			final int maxNumCacheEntries) throws IOException
	{
		return openVolatileWithBoundedSoftRefCache(n5, dataset, (Consumer<IterableInterval<T>>)img -> {}, maxNumCacheEntries);
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
	public static final <T extends NativeType<T>> RandomAccessibleInterval<T> openWithDiskCache(
			final N5Reader n5,
			final String dataset) throws IOException {

		return openWithDiskCache(n5, dataset, (Consumer<IterableInterval<T>>)img -> {});
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
	public static final <T extends NativeType<T>> RandomAccessibleInterval<T> open(
			final N5Reader n5,
			final String dataset,
			final T defaultValue) throws IOException {

		return open(n5, dataset, N5CellLoader.setToDefaultValue(defaultValue));
	}

	/**
	 * Open an N5 dataset as a memory cached {@link LazyCellImg}.
	 *
	 * @param n5
	 * @param dataset
	 * @param defaultValue
	 * @param maxNumCacheEntries
	 * @return
	 * @throws IOException
	 */
	public static final <T extends NativeType<T>> RandomAccessibleInterval<T> openWithBoundedSoftRefCache(
			final N5Reader n5,
			final String dataset,
			final int maxNumCacheEntries,
			final T defaultValue) throws IOException {

		return openWithBoundedSoftRefCache(n5, dataset, N5CellLoader.setToDefaultValue(defaultValue), maxNumCacheEntries);
	}

	/**
	 * Open an N5 dataset as a memory cached {@link LazyCellImg} using
	 * {@link VolatileAccess}.
	 *
	 * @param n5
	 * @param dataset
	 * @param defaultValue
	 * @return
	 * @throws IOException
	 */
	public static final <T extends NativeType<T>> RandomAccessibleInterval<T> openVolatile(
			final N5Reader n5,
			final String dataset,
			final T defaultValue) throws IOException {

		return openVolatile(n5, dataset, N5CellLoader.setToDefaultValue(defaultValue));
	}

	/**
	 * Open an N5 dataset as a memory cached {@link LazyCellImg} using
	 * {@link VolatileAccess}.
	 *
	 * @param n5
	 * @param dataset
	 * @param defaultValue
	 * @param maxNumCacheEntries
	 * @return
	 * @throws IOException
	 */
	public static final <T extends NativeType<T>> RandomAccessibleInterval<T> openVolatileWithBoundedSoftRefCache(
			final N5Reader n5,
			final String dataset,
			final int maxNumCacheEntries,
			final T defaultValue) throws IOException {

		return openVolatileWithBoundedSoftRefCache(n5, dataset, N5CellLoader.setToDefaultValue(defaultValue), maxNumCacheEntries);
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
	public static final <T extends NativeType<T>> RandomAccessibleInterval<T> openWithDiskCache(
			final N5Reader n5,
			final String dataset,
			final T defaultValue) throws IOException {

		return openWithDiskCache(n5, dataset, N5CellLoader.setToDefaultValue(defaultValue));
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
	@SuppressWarnings({"unchecked", "rawtypes"})
	public static final <T extends NativeType<T>> RandomAccessibleInterval<T> open(
			final N5Reader n5,
			final String dataset,
			final Consumer<IterableInterval<T>> blockNotFoundHandler) throws IOException {
		return open(n5, dataset, blockNotFoundHandler, AccessFlags.setOf());
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
	@SuppressWarnings({"unchecked", "rawtypes"})
	public static final <T extends NativeType<T>> RandomAccessibleInterval<T> open(
			final N5Reader n5,
			final String dataset,
			final Consumer<IterableInterval<T>> blockNotFoundHandler,
			final Set<AccessFlags> accessFlags) throws IOException {
		return open(n5, dataset, blockNotFoundHandler, dataType -> new SoftRefLoaderCache(), accessFlags);
	}

	/**
	 * Open an N5 dataset as a memory cached {@link LazyCellImg} with a bound on the number of cache entries.
	 *
	 * @param n5
	 * @param dataset
	 * @param blockNotFoundHandler
	 * @param maxNumCacheEntries
	 * @return
	 * @throws IOException
	 */
	@SuppressWarnings({"unchecked", "rawtypes"})
	public static final <T extends NativeType<T>> RandomAccessibleInterval<T> openWithBoundedSoftRefCache(
			final N5Reader n5,
			final String dataset,
			final Consumer<IterableInterval<T>> blockNotFoundHandler,
			final int maxNumCacheEntries) throws IOException {
		return openWithBoundedSoftRefCache(n5, dataset, blockNotFoundHandler, maxNumCacheEntries, AccessFlags.setOf());
	}

	/**
	 * Open an N5 dataset as a memory cached {@link LazyCellImg} with a bound on the number of cache entries.
	 *
	 * @param n5
	 * @param dataset
	 * @param blockNotFoundHandler
	 * @param maxNumCacheEntries
	 * @return
	 * @throws IOException
	 */
	@SuppressWarnings({"unchecked", "rawtypes"})
	public static final <T extends NativeType<T>> RandomAccessibleInterval<T> openWithBoundedSoftRefCache(
			final N5Reader n5,
			final String dataset,
			final Consumer<IterableInterval<T>> blockNotFoundHandler,
			final int maxNumCacheEntries,
			final Set<AccessFlags> accessFlags) throws IOException {
		return open(n5, dataset, blockNotFoundHandler, dataType -> new BoundedSoftRefLoaderCache(maxNumCacheEntries), accessFlags);
	}

	/**
	 * Open an N5 dataset as a memory cached {@link LazyCellImg}.
	 *
	 * @param n5
	 * @param dataset
	 * @param blockNotFoundHandler
	 * @param loaderCacheFactory
	 * @return
	 * @throws IOException
	 */
	@SuppressWarnings({"unchecked", "rawtypes"})
	public static final <T extends NativeType<T>> RandomAccessibleInterval<T> open(
			final N5Reader n5,
			final String dataset,
			final Consumer<IterableInterval<T>> blockNotFoundHandler,
			final Function<DataType, LoaderCache> loaderCacheFactory,
			final Set<AccessFlags> accessFlags) throws IOException {

		final DatasetAttributes attributes = n5.getDatasetAttributes(dataset);
		final LoaderCache loaderCache = loaderCacheFactory.apply(attributes.getDataType());
		final T type = type(attributes.getDataType());
		return type == null
				? null
				: open(n5, dataset, blockNotFoundHandler, loaderCache, accessFlags, type);
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
	@SuppressWarnings({"unchecked", "rawtypes"})
	public static final <T extends NativeType<T>, A extends ArrayDataAccess<A>> CachedCellImg<T, A> open(
			final N5Reader n5,
			final String dataset,
			final Consumer<IterableInterval<T>> blockNotFoundHandler,
			final LoaderCache<Long, Cell<A>> loaderCache,
			final Set<AccessFlags> accessFlags,
			final T type) throws IOException {

		final DatasetAttributes attributes = n5.getDatasetAttributes(dataset);
		final long[] dimensions = attributes.getDimensions();
		final int[] blockSize = attributes.getBlockSize();

		final N5CellLoader<T> loader = new N5CellLoader<>(n5, dataset, blockSize, blockNotFoundHandler);

		final CellGrid grid = new CellGrid(dimensions, blockSize);

		final Cache<Long, Cell<A>> cache = loaderCache.withLoader(LoadedCellCacheLoader.get(grid, loader, type, accessFlags));
		final CachedCellImg img = new CachedCellImg(grid, type, cache, ArrayDataAccessFactory.get(type, accessFlags));
		return img;
	}

	/**
	 * Open an N5 dataset as a memory cached {@link LazyCellImg} using
	 * {@link VolatileAccess}.
	 *
	 * @param n5
	 * @param dataset
	 * @param blockNotFoundHandler
	 * @return
	 * @throws IOException
	 */
	@SuppressWarnings({"unchecked", "rawtypes"})
	public static final <T extends NativeType<T>> RandomAccessibleInterval<T> openVolatile(
			final N5Reader n5,
			final String dataset,
			final Consumer<IterableInterval<T>> blockNotFoundHandler) throws IOException {
		return open(n5, dataset, blockNotFoundHandler, AccessFlags.setOf(AccessFlags.VOLATILE));
	}

	/**
	 * Open an N5 dataset as a memory cached {@link LazyCellImg} with a bound on the number of cache entries
	 * using {@link VolatileAccess}.
	 *
	 * @param n5
	 * @param dataset
	 * @param blockNotFoundHandler
	 * @param maxNumCacheEntries
	 * @return
	 * @throws IOException
	 */
	public static <T extends NativeType<T>> RandomAccessibleInterval<T> openVolatileWithBoundedSoftRefCache(
			final N5Reader n5,
			final String dataset,
			final Consumer<IterableInterval<T>> blockNotFoundHandler,
			final int maxNumCacheEntries) throws IOException {
		return openWithBoundedSoftRefCache(n5, dataset, blockNotFoundHandler, maxNumCacheEntries, AccessFlags.setOf(AccessFlags.VOLATILE));
	}

	/**
	 * Open an N5 mipmap (multi-scale) group as memory cached
	 * {@link LazyCellImg}s, optionally backed by {@link VolatileAccess}.
	 *
	 * @param n5
	 * @param group
	 * @param useVolatileAccess
	 * @param blockNotFoundHandlerSupplier
	 *
	 * @return
	 * @throws IOException
	 */
	public static final <T extends NativeType<T>> Pair<RandomAccessibleInterval<T>[], double[][]> openMipmapsWithHandler(
			final N5Reader n5,
			final String group,
			final boolean useVolatileAccess,
			final IntFunction<Consumer<IterableInterval<T>>> blockNotFoundHandlerSupplier) throws IOException {

		final int numScales = n5.list(group).length;
		@SuppressWarnings("unchecked")
		final RandomAccessibleInterval<T>[] mipmaps = new RandomAccessibleInterval[numScales];
		final double[][] scales = new double[numScales][];

		for (int s = 0; s < numScales; ++s) {
			final String datasetName = group + "/s" + s;
			final long[] dimensions = n5.getAttribute(datasetName, "dimensions", long[].class);
			final long[] downsamplingFactors = n5.getAttribute(datasetName, "downsamplingFactors", long[].class);
			final double[] scale = new double[dimensions.length];
			if (downsamplingFactors == null) {
				final int si = 1 << s;
				for (int i = 0; i < scale.length; ++i)
					scale[i] = si;
			} else {
				for (int i = 0; i < scale.length; ++i)
					scale[i] = downsamplingFactors[i];
			}

			final RandomAccessibleInterval<T> source;
			if (useVolatileAccess)
				source = N5Utils.openVolatile(n5, datasetName, blockNotFoundHandlerSupplier.apply(s));
			else
				source = N5Utils.open(n5, datasetName, blockNotFoundHandlerSupplier.apply(s));

			mipmaps[s] = source;
			scales[s] = scale;
		}

		return new ValuePair<>(mipmaps, scales);
	}

	/**
	 * Open an N5 mipmap (multi-scale) group as memory cached
	 * {@link LazyCellImg}s, optionally backed by {@link VolatileAccess}.
	 *
	 * @param n5
	 * @param group
	 * @param useVolatileAccess
	 * @param defaultValueSupplier
	 *
	 * @return
	 * @throws IOException
	 */
	public static final <T extends NativeType<T>> Pair<RandomAccessibleInterval<T>[], double[][]> openMipmaps(
			final N5Reader n5,
			final String group,
			final boolean useVolatileAccess,
			final IntFunction<T> defaultValueSupplier) throws IOException {

		return openMipmapsWithHandler(
				n5,
				group,
				useVolatileAccess,
				s -> {
					return N5CellLoader.setToDefaultValue(defaultValueSupplier.apply(s));
				});
	}

	/**
	 * Open an N5 mipmap (multi-scale) group as memory cached
	 * {@link LazyCellImg}s, optionally backed by {@link VolatileAccess}.
	 *
	 * @param n5
	 * @param group
	 * @param useVolatileAccess
	 *
	 * @return
	 * @throws IOException
	 */
	public static final <T extends NativeType<T>> Pair<RandomAccessibleInterval<T>[], double[][]> openMipmaps(
			final N5Reader n5,
			final String group,
			final boolean useVolatileAccess) throws IOException {

		return openMipmapsWithHandler(
				n5,
				group,
				useVolatileAccess,
				s -> t -> {});
	}

	/**
	 * Open an N5 dataset as a disk-cached {@link LazyCellImg}. Note that this
	 * requires that al part of the the N5 dataset that will be accessed fit
	 * into /tmp.
	 *
	 * @param n5
	 * @param dataset
	 * @param blockNotFoundHandler
	 * @return
	 * @throws IOException
	 */
	public static final <T extends NativeType<T>> RandomAccessibleInterval<T> openWithDiskCache(
			final N5Reader n5,
			final String dataset,
			final Consumer<IterableInterval<T>> blockNotFoundHandler) throws IOException {

		final DatasetAttributes attributes = n5.getDatasetAttributes(dataset);
		final long[] dimensions = attributes.getDimensions();
		final int[] blockSize = attributes.getBlockSize();

		final N5CellLoader<T> loader = new N5CellLoader<>(n5, dataset, blockSize, blockNotFoundHandler);

		final DiskCachedCellImgOptions options = DiskCachedCellImgOptions
				.options()
				.cellDimensions(blockSize)
				.dirtyAccesses(true)
				.maxCacheSize(100);

		final DiskCachedCellImgFactory<T> factory = new DiskCachedCellImgFactory<T>(
				type(attributes.getDataType()),
				options);

		return factory.create(dimensions, loader);
	}

	/**
	 * Save a {@link RandomAccessibleInterval} into an N5 dataset at a given
	 * offset. The offset is given in {@link DataBlock} grid coordinates and the
	 * source is assumed to align with the {@link DataBlock} grid of the dataset.
	 *
	 * @param source
	 * @param n5
	 * @param dataset
	 * @param attributes
	 * @param gridOffset
	 * @throws IOException
	 */
	public static final <T extends NativeType<T>> void saveBlock(
			RandomAccessibleInterval<T> source,
			final N5Writer n5,
			final String dataset,
			final DatasetAttributes attributes,
			final long[] gridOffset) throws IOException {

		source = Views.zeroMin(source);
		final int n = source.numDimensions();
		final long[] max = Intervals.maxAsLongArray(source);
		final long[] offset = new long[n];
		final long[] gridPosition = new long[n];
		final int[] blockSize = attributes.getBlockSize();
		final int[] intCroppedBlockSize = new int[n];
		final long[] longCroppedBlockSize = new long[n];
		for (int d = 0; d < n;) {
			cropBlockDimensions(
					max,
					offset,
					gridOffset,
					blockSize,
					longCroppedBlockSize,
					intCroppedBlockSize,
					gridPosition);
			final RandomAccessibleInterval<T> sourceBlock = Views.offsetInterval(source, offset, longCroppedBlockSize);
			final DataBlock<?> dataBlock = createDataBlock(
					sourceBlock,
					attributes.getDataType(),
					intCroppedBlockSize,
					longCroppedBlockSize,
					gridPosition);

			n5.writeBlock(dataset, attributes, dataBlock);

			for (d = 0; d < n; ++d) {
				offset[d] += blockSize[d];
				if (offset[d] <= max[d])
					break;
				else
					offset[d] = 0;
			}
		}
	}

	/**
	 * Save a {@link RandomAccessibleInterval} into an N5 dataset.
	 * The block offset is determined by the source position, and the
	 * source is assumed to align with the {@link DataBlock} grid
	 * of the dataset.
	 *
	 * @param source
	 * @param n5
	 * @param dataset
	 * @param attributes
	 * @throws IOException
	 */
	public static final <T extends NativeType<T>> void saveBlock(
			final RandomAccessibleInterval<T> source,
			final N5Writer n5,
			final String dataset,
			final DatasetAttributes attributes) throws IOException {

		final int[] blockSize = attributes.getBlockSize();
		final long[] gridOffset = new long[blockSize.length];
		Arrays.setAll(gridOffset, d -> source.min(d) / blockSize[d]);
		saveBlock(source, n5, dataset, attributes, gridOffset);
	}

	/**
	 * Save a {@link RandomAccessibleInterval} into an N5 dataset.
	 * The block offset is determined by the source position, and the
	 * source is assumed to align with the {@link DataBlock} grid
	 * of the dataset.
	 *
	 * @param source
	 * @param n5
	 * @param dataset
	 * @throws IOException
	 */
	public static final <T extends NativeType<T>> void saveBlock(
			final RandomAccessibleInterval<T> source,
			final N5Writer n5,
			final String dataset) throws IOException {

		final DatasetAttributes attributes = n5.getDatasetAttributes(dataset);
		if (attributes != null) {
			saveBlock(source, n5, dataset, attributes);
		} else {
			throw new IOException("Dataset " + dataset + " does not exist.");
		}
	}

	/**
	 * Save a {@link RandomAccessibleInterval} into an N5 dataset at a given
	 * offset. The offset is given in {@link DataBlock} grid coordinates and the
	 * source is assumed to align with the {@link DataBlock} grid of the dataset.
	 *
	 * @param source
	 * @param n5
	 * @param dataset
	 * @param gridOffset
	 * @throws IOException
	 */
	public static final <T extends NativeType<T>> void saveBlock(
			final RandomAccessibleInterval<T> source,
			final N5Writer n5,
			final String dataset,
			final long[] gridOffset) throws IOException {

		final DatasetAttributes attributes = n5.getDatasetAttributes(dataset);
		if (attributes != null) {
			saveBlock(source, n5, dataset, attributes, gridOffset);
		} else {
			throw new IOException("Dataset " + dataset + " does not exist.");
		}
	}

	/**
	 * Save a {@link RandomAccessibleInterval} as an N5 dataset, multi-threaded.
	 *
	 * @param source
	 * @param n5
	 * @param dataset
	 * @param gridOffset
	 * @param exec
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	public static final <T extends NativeType<T>> void saveBlock(
			final RandomAccessibleInterval<T> source,
			final N5Writer n5,
			final String dataset,
			final long[] gridOffset,
			final ExecutorService exec) throws IOException, InterruptedException, ExecutionException {

		final RandomAccessibleInterval<T> zeroMinSource = Views.zeroMin(source);
		final long[] dimensions = Intervals.dimensionsAsLongArray(zeroMinSource);
		final DatasetAttributes attributes = n5.getDatasetAttributes(dataset);
		if (attributes != null) {
			final int n = dimensions.length;
			final long[] max = Intervals.maxAsLongArray(zeroMinSource);
			final long[] offset = new long[n];
			final int[] blockSize = attributes.getBlockSize();

			final ArrayList<Future<?>> futures = new ArrayList<>();
			for (int d = 0; d < n;) {
				final long[] fOffset = offset.clone();

				futures.add(
						exec.submit(
								() -> {

									final long[] gridPosition = new long[n];
									final int[] intCroppedBlockSize = new int[n];
									final long[] longCroppedBlockSize = new long[n];

									cropBlockDimensions(
											max,
											fOffset,
											gridOffset,
											blockSize,
											longCroppedBlockSize,
											intCroppedBlockSize,
											gridPosition);

									final RandomAccessibleInterval<T> sourceBlock = Views
											.offsetInterval(zeroMinSource, fOffset, longCroppedBlockSize);
									final DataBlock<?> dataBlock = createDataBlock(
											sourceBlock,
											attributes.getDataType(),
											intCroppedBlockSize,
											longCroppedBlockSize,
											gridPosition);

									try {
										n5.writeBlock(dataset, attributes, dataBlock);
									} catch (final IOException e) {
										e.printStackTrace();
									}
								}));

				for (d = 0; d < n; ++d) {
					offset[d] += blockSize[d];
					if (offset[d] <= max[d])
						break;
					else
						offset[d] = 0;
				}
			}
			for (final Future<?> f : futures)
				f.get();
		} else {
			throw new IOException("Dataset " + dataset + " does not exist.");
		}
	}

	/**
	 * Save a {@link RandomAccessibleInterval} into an N5 dataset at a given
	 * offset. The offset is given in {@link DataBlock} grid coordinates and the
	 * source is assumed to align with the {@link DataBlock} grid of the
	 * dataset. Only {@link DataBlock DataBlocks} that contain values other than
	 * a given default value are stored.
	 *
	 * @param source
	 * @param n5
	 * @param dataset
	 * @param attributes
	 * @param gridOffset
	 * @param defaultValue
	 * @throws IOException
	 */
	public static final <T extends NativeType<T>> void saveNonEmptyBlock(
			RandomAccessibleInterval<T> source,
			final N5Writer n5,
			final String dataset,
			final DatasetAttributes attributes,
			final long[] gridOffset,
			final T defaultValue) throws IOException {

		source = Views.zeroMin(source);
		final int n = source.numDimensions();
		final long[] max = Intervals.maxAsLongArray(source);
		final long[] offset = new long[n];
		final long[] gridPosition = new long[n];
		final int[] blockSize = attributes.getBlockSize();
		final int[] intCroppedBlockSize = new int[n];
		final long[] longCroppedBlockSize = new long[n];
		for (int d = 0; d < n;) {
			cropBlockDimensions(
					max,
					offset,
					gridOffset,
					blockSize,
					longCroppedBlockSize,
					intCroppedBlockSize,
					gridPosition);
			final RandomAccessibleInterval<T> sourceBlock = Views.offsetInterval(source, offset, longCroppedBlockSize);
			final DataBlock<?> dataBlock = createNonEmptyDataBlock(
					sourceBlock,
					attributes.getDataType(),
					intCroppedBlockSize,
					longCroppedBlockSize,
					gridPosition,
					defaultValue);

			if (dataBlock != null)
				n5.writeBlock(dataset, attributes, dataBlock);

			for (d = 0; d < n; ++d) {
				offset[d] += blockSize[d];
				if (offset[d] <= max[d])
					break;
				else
					offset[d] = 0;
			}
		}
	}

	/**
	 * Save a {@link RandomAccessibleInterval} into an N5 dataset.
	 * The block offset is determined by the source position, and the
	 * source is assumed to align with the {@link DataBlock} grid
	 * of the dataset. Only {@link DataBlock DataBlocks} that contain
	 * values other than a given default value are stored.
	 *
	 * @param source
	 * @param n5
	 * @param dataset
	 * @param attributes
	 * @param defaultValue
	 * @throws IOException
	 */
	public static final <T extends NativeType<T>> void saveNonEmptyBlock(
			final RandomAccessibleInterval<T> source,
			final N5Writer n5,
			final String dataset,
			final DatasetAttributes attributes,
			final T defaultValue) throws IOException {

		final int[] blockSize = attributes.getBlockSize();
		final long[] gridOffset = new long[blockSize.length];
		Arrays.setAll(gridOffset, d -> source.min(d) / blockSize[d]);
		saveNonEmptyBlock(source, n5, dataset, attributes, gridOffset, defaultValue);
	}

	/**
	 * Save a {@link RandomAccessibleInterval} into an N5 dataset.
	 * The block offset is determined by the source position, and the
	 * source is assumed to align with the {@link DataBlock} grid
	 * of the dataset. Only {@link DataBlock DataBlocks} that contain
	 * values other than a given default value are stored.
	 *
	 * @param source
	 * @param n5
	 * @param dataset
	 * @param defaultValue
	 * @throws IOException
	 */
	public static final <T extends NativeType<T>> void saveNonEmptyBlock(
			final RandomAccessibleInterval<T> source,
			final N5Writer n5,
			final String dataset,
			final T defaultValue) throws IOException {

		final DatasetAttributes attributes = n5.getDatasetAttributes(dataset);
		if (attributes != null) {
			saveNonEmptyBlock(source, n5, dataset, attributes, defaultValue);
		} else {
			throw new IOException("Dataset " + dataset + " does not exist.");
		}
	}

	/**
	 * Save a {@link RandomAccessibleInterval} into an N5 dataset at a given
	 * offset. The offset is given in {@link DataBlock} grid coordinates and the
	 * source is assumed to align with the {@link DataBlock} grid of the
	 * dataset. Only {@link DataBlock DataBlocks} that contain values other than
	 * a given default value are stored.
	 *
	 * @param source
	 * @param n5
	 * @param dataset
	 * @param gridOffset
	 * @param defaultValue
	 * @throws IOException
	 */
	public static final <T extends NativeType<T>> void saveNonEmptyBlock(
			final RandomAccessibleInterval<T> source,
			final N5Writer n5,
			final String dataset,
			final long[] gridOffset,
			final T defaultValue) throws IOException {

		final DatasetAttributes attributes = n5.getDatasetAttributes(dataset);
		if (attributes != null) {
			saveNonEmptyBlock(source, n5, dataset, attributes, gridOffset, defaultValue);
		} else {
			throw new IOException("Dataset " + dataset + " does not exist.");
		}
	}

	/**
	 * Save a {@link RandomAccessibleInterval} as an N5 dataset.
	 *
	 * @param source
	 * @param n5
	 * @param dataset
	 * @param blockSize
	 * @param compression
	 * @throws IOException
	 */
	public static final <T extends NativeType<T>> void save(
			RandomAccessibleInterval<T> source,
			final N5Writer n5,
			final String dataset,
			final int[] blockSize,
			final Compression compression) throws IOException {

		source = Views.zeroMin(source);
		final long[] dimensions = Intervals.dimensionsAsLongArray(source);
		final DatasetAttributes attributes = new DatasetAttributes(
				dimensions,
				blockSize,
				dataType(Util.getTypeFromInterval(source)),
				compression);

		n5.createDataset(dataset, attributes);

		final int n = dimensions.length;
		final long[] max = Intervals.maxAsLongArray(source);
		final long[] offset = new long[n];
		final long[] gridPosition = new long[n];
		final int[] intCroppedBlockSize = new int[n];
		final long[] longCroppedBlockSize = new long[n];
		for (int d = 0; d < n;) {
			cropBlockDimensions(max, offset, blockSize, longCroppedBlockSize, intCroppedBlockSize, gridPosition);
			final RandomAccessibleInterval<T> sourceBlock = Views.offsetInterval(source, offset, longCroppedBlockSize);
			final DataBlock<?> dataBlock = createDataBlock(
					sourceBlock,
					attributes.getDataType(),
					intCroppedBlockSize,
					longCroppedBlockSize,
					gridPosition);

			n5.writeBlock(dataset, attributes, dataBlock);

			for (d = 0; d < n; ++d) {
				offset[d] += blockSize[d];
				if (offset[d] <= max[d])
					break;
				else
					offset[d] = 0;
			}
		}
	}

	/**
	 * Save a {@link RandomAccessibleInterval} as an N5 dataset, multi-threaded.
	 *
	 * @param source
	 * @param n5
	 * @param dataset
	 * @param blockSize
	 * @param compression
	 * @param exec
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	public static final <T extends NativeType<T>> void save(
			final RandomAccessibleInterval<T> source,
			final N5Writer n5,
			final String dataset,
			final int[] blockSize,
			final Compression compression,
			final ExecutorService exec) throws IOException, InterruptedException, ExecutionException {

		final RandomAccessibleInterval<T> zeroMinSource = Views.zeroMin(source);
		final long[] dimensions = Intervals.dimensionsAsLongArray(zeroMinSource);
		final DatasetAttributes attributes = new DatasetAttributes(
				dimensions,
				blockSize,
				dataType(Util.getTypeFromInterval(zeroMinSource)),
				compression);

		n5.createDataset(dataset, attributes);

		final int n = dimensions.length;
		final long[] max = Intervals.maxAsLongArray(zeroMinSource);
		final long[] offset = new long[n];

		final ArrayList<Future<?>> futures = new ArrayList<>();
		for (int d = 0; d < n;) {
			final long[] fOffset = offset.clone();

			futures.add(
					exec.submit(
							() -> {

								final long[] gridPosition = new long[n];
								final int[] intCroppedBlockSize = new int[n];
								final long[] longCroppedBlockSize = new long[n];

								cropBlockDimensions(
										max,
										fOffset,
										blockSize,
										longCroppedBlockSize,
										intCroppedBlockSize,
										gridPosition);

								final RandomAccessibleInterval<T> sourceBlock = Views
										.offsetInterval(zeroMinSource, fOffset, longCroppedBlockSize);
								final DataBlock<?> dataBlock = createDataBlock(
										sourceBlock,
										attributes.getDataType(),
										intCroppedBlockSize,
										longCroppedBlockSize,
										gridPosition);

								try {
									n5.writeBlock(dataset, attributes, dataBlock);
								} catch (final IOException e) {
									e.printStackTrace();
								}
							}));

			for (d = 0; d < n; ++d) {
				offset[d] += blockSize[d];
				if (offset[d] <= max[d])
					break;
				else
					offset[d] = 0;
			}
		}
		for (final Future<?> f : futures)
			f.get();
	}
}

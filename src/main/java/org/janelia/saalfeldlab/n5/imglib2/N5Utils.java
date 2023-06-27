/**
 * Copyright (c) 2017-2021, Saalfeld lab, HHMI Janelia
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
import java.util.Optional;
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

import net.imglib2.FinalInterval;
import net.imglib2.Interval;
import net.imglib2.IterableInterval;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.cache.Cache;
import net.imglib2.cache.CacheLoader;
import net.imglib2.cache.LoaderCache;
import net.imglib2.cache.img.CachedCellImg;
import net.imglib2.cache.img.DiskCachedCellImgFactory;
import net.imglib2.cache.img.DiskCachedCellImgOptions;
import net.imglib2.cache.ref.BoundedSoftRefLoaderCache;
import net.imglib2.cache.ref.SoftRefLoaderCache;
import net.imglib2.exception.ImgLibException;
import net.imglib2.img.Img;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.basictypeaccess.AccessFlags;
import net.imglib2.img.basictypeaccess.ArrayDataAccessFactory;
import net.imglib2.img.basictypeaccess.array.ArrayDataAccess;
import net.imglib2.img.basictypeaccess.volatiles.VolatileAccess;
import net.imglib2.img.cell.Cell;
import net.imglib2.img.cell.CellGrid;
import net.imglib2.img.cell.LazyCellImg;
import net.imglib2.iterator.IntervalIterator;
import net.imglib2.loops.LoopBuilder;
import net.imglib2.type.NativeType;
import net.imglib2.type.Type;
import net.imglib2.type.label.LabelMultisetType;
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
import net.imglib2.view.IntervalView;
import net.imglib2.view.Views;

/**
 * Static utility methods to open N5 datasets as ImgLib2
 * {@link RandomAccessibleInterval RandomAccessibleIntervals} and to save
 * ImgLib2 {@link RandomAccessibleInterval RandomAccessibleIntervals} as
 * [sparse] N5 datasets.
 *
 * @author Stephan Saalfeld &lt;saalfelds@janelia.hhmi.org&gt;
 * @author Philipp Hanslovsky &lt;hanslovskyp@janelia.hhmi.org&gt;
 * @author John Bogovic &lt;bogovicj@janelia.hhmi.org&gt;
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
			return (T)new ByteType();
		case UINT8:
			return (T)new UnsignedByteType();
		case INT16:
			return (T)new ShortType();
		case UINT16:
			return (T)new UnsignedShortType();
		case INT32:
			return (T)new IntType();
		case UINT32:
			return (T)new UnsignedIntType();
		case INT64:
			return (T)new LongType();
		case UINT64:
			return (T)new UnsignedLongType();
		case FLOAT32:
			return (T)new FloatType();
		case FLOAT64:
			return (T)new DoubleType();
		default:
			return null;
		}
	}

	/**
	 * Creates a {@link DataBlock} of matching type and copies the content of
	 * source into it. This is a helper method with redundant parameters.
	 *
	 * @param source the source image
	 * @param dataType the datatype
	 * @param intBlockSize the block size as an int array
	 * @param longBlockSize the block size as a long array
	 * @param gridPosition the grid position of the block
	 * @return the data block
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
	 * @param <T> the type parameter
	 * @param source the source block 
	 * @param dataType the data type
	 * @param intBlockSize the block since as an int array
	 * @param longBlockSize the block since as a long array
	 * @param gridPosition the grid position
	 * @param defaultValue the default value
	 * @return the data block
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
	 * @param max the max coordinate of the dataset
	 * @param offset the block offset
	 * @param blockDimensions the block size
	 * @param croppedBlockDimensions the cropped block size as a long array
	 * @param intCroppedBlockDimensions the cropped block size as an int array
	 * @param gridPosition the grid position
	 */
	static void cropBlockDimensions(
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
	 * @param max the max coordinate of the dataset
	 * @param offset the block offsett
	 * @param gridOffset the grid offset
	 * @param blockDimensions the block dimensions
	 * @param croppedBlockDimensions the cropped block dimensions as a long array
	 * @param intCroppedBlockDimensions the cropped block dimensions as an int array
	 * @param gridPosition the block grid position
	 */
	static void cropBlockDimensions(
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
	 * Open an N5 dataset as a memory cached {@link LazyCellImg}. Supports all
	 * primitive types and {@link LabelMultisetType}.
	 *
	 * @param <T> the type parameter
	 * @param n5 the n5 reader
	 * @param dataset the dataset path
	 * @return the image
	 * @throws IOException the exception
	 */
	@SuppressWarnings("unchecked")
	public static final <T extends NativeType<T>> CachedCellImg<T, ?> open(
			final N5Reader n5,
			final String dataset) throws IOException {

		if (N5LabelMultisets.isLabelMultisetType(n5, dataset))
			return (CachedCellImg<T, ?>)N5LabelMultisets.openLabelMultiset(n5, dataset);
		else
			return open(n5, dataset, (Consumer<IterableInterval<T>>)img -> {});
	}

	/**
	 * Open an N5 dataset as a memory cached {@link LazyCellImg}.
	 * 
	 * @param <T> the type parameter
	 * @param n5 the n5 reader
	 * @param dataset the dataset path
	 * @param maxNumCacheEntries the max number of cache entries
	 * @return the image
	 * @throws IOException the exception
	 */
	public static final <T extends NativeType<T>> CachedCellImg<T, ?> openWithBoundedSoftRefCache(
			final N5Reader n5,
			final String dataset,
			final int maxNumCacheEntries) throws IOException {

		return openWithBoundedSoftRefCache(n5, dataset, (Consumer<IterableInterval<T>>)img -> {}, maxNumCacheEntries);
	}

	/**
	 * Open an N5 dataset as a memory cached {@link LazyCellImg} using
	 * {@link VolatileAccess}. Supports all primitive types and
	 * {@link LabelMultisetType}.
	 *
	 * @param <T> the type parameter
	 * @param n5 the n5 reader
	 * @param dataset the dataset path
	 * @return the image
	 * @throws IOException the exception
	 */
	@SuppressWarnings("unchecked")
	public static final <T extends NativeType<T>> CachedCellImg<T, ?> openVolatile(
			final N5Reader n5,
			final String dataset) throws IOException {

		if (N5LabelMultisets.isLabelMultisetType(n5, dataset))
			return (CachedCellImg<T, ?>)N5LabelMultisets.openLabelMultiset(n5, dataset);
		else
			return openVolatile(n5, dataset, (Consumer<IterableInterval<T>>)img -> {});
	}

	/**
	 * Open an N5 dataset as a memory cached {@link LazyCellImg} using
	 * {@link VolatileAccess}.
	 *
	 * @param <T> the type parameter
	 * @param n5 n5 reader
	 * @param dataset the dataset path
	 * @param maxNumCacheEntries the max number of cache entries
	 * @return the image
	 * @throws IOException the exception
	 */
	public static final <T extends NativeType<T>> CachedCellImg<T, ?> openVolatileWithBoundedSoftRefCache(
			final N5Reader n5,
			final String dataset,
			final int maxNumCacheEntries) throws IOException {

		return openVolatileWithBoundedSoftRefCache(n5, dataset, (Consumer<IterableInterval<T>>)img -> {}, maxNumCacheEntries);
	}

	/**
	 * Open an N5 dataset as a disk-cached {@link LazyCellImg}. Note that this
	 * requires that all parts of the the N5 dataset that will be accessed fit
	 * into /tmp.
	 *
	 * @param <T> the type parameter
	 * @param n5 n5 reader
	 * @param dataset the dataset path
	 * @return the image
	 * @throws IOException the exception
	 */
	public static final <T extends NativeType<T>> CachedCellImg<T, ?> openWithDiskCache(
			final N5Reader n5,
			final String dataset) throws IOException {

		return openWithDiskCache(n5, dataset, (Consumer<IterableInterval<T>>)img -> {});
	}

	/**
	 * Open an N5 dataset as a memory cached {@link LazyCellImg}.
	 *
	 * @param <T> the type parameter
	 * @param n5 n5 reader
	 * @param dataset the dataset path
	 * @param defaultValue the default value
	 * @return the image
	 * @throws IOException the exception
	 */
	public static final <T extends NativeType<T>> CachedCellImg<T, ?> open(
			final N5Reader n5,
			final String dataset,
			final T defaultValue) throws IOException {

		return open(n5, dataset, N5CellLoader.setToDefaultValue(defaultValue));
	}

	/**
	 * Open an N5 dataset as a memory cached {@link LazyCellImg}.
	 *
	 * @param <T> the type parameter
	 * @param n5 n5 reader
	 * @param dataset the dataset path
	 * @param defaultValue the default value
	 * @param maxNumCacheEntries the max number of cache entries
	 * @return the image
	 * @throws IOException the exception
	 */
	public static final <T extends NativeType<T>> CachedCellImg<T, ?> openWithBoundedSoftRefCache(
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
	 * @param <T> the type parameter
	 * @param n5 n5 reader
	 * @param dataset the dataset path
	 * @param defaultValue the default value
	 * @return the image
	 * @throws IOException the exception
	 */
	public static final <T extends NativeType<T>> CachedCellImg<T, ?> openVolatile(
			final N5Reader n5,
			final String dataset,
			final T defaultValue) throws IOException {

		return openVolatile(n5, dataset, N5CellLoader.setToDefaultValue(defaultValue));
	}

	/**
	 * Open an N5 dataset as a memory cached {@link LazyCellImg} using
	 * {@link VolatileAccess}.
	 *
	 * @param <T> the type parameter
	 * @param n5 n5 reader
	 * @param dataset the dataset path
	 * @param defaultValue the default value
	 * @param maxNumCacheEntries the maximum number of cache entries
	 * @return the image
	 * @throws IOException the exception
	 */
	public static final <T extends NativeType<T>> CachedCellImg<T, ?> openVolatileWithBoundedSoftRefCache(
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
	 * @param <T> the type parameter
	 * @param n5 n5 reader
	 * @param dataset the dataset path
	 * @param defaultValue the default value
	 * @return the image
	 * @throws IOException the exception
	 */
	public static final <T extends NativeType<T>> CachedCellImg<T, ?> openWithDiskCache(
			final N5Reader n5,
			final String dataset,
			final T defaultValue) throws IOException {

		return openWithDiskCache(n5, dataset, N5CellLoader.setToDefaultValue(defaultValue));
	}

	/**
	 * Open an N5 dataset as a memory cached {@link LazyCellImg}.
	 *
	 * @param <T> the type
	 * @param n5 n5 reader
	 * @param dataset the dataset path
	 * @param blockNotFoundHandler consumer handling missing blocks 
	 * @return the image
	 * @throws IOException the exception
	 */
	public static final <T extends NativeType<T>> CachedCellImg<T, ?> open(
			final N5Reader n5,
			final String dataset,
			final Consumer<IterableInterval<T>> blockNotFoundHandler) throws IOException {

		return open(n5, dataset, blockNotFoundHandler, AccessFlags.setOf());
	}

	/**
	 * Open an N5 dataset as a memory cached {@link LazyCellImg}.
	 *
	 * @param <T> the type
	 * @param n5 n5 reader
	 * @param dataset the dataset path
	 * @param blockNotFoundHandler consumer handling missing blocks
	 * @param accessFlags the access flag set
	 * @return the image
	 * @throws IOException the exception
	 */
	public static final <T extends NativeType<T>> CachedCellImg<T, ?> open(
			final N5Reader n5,
			final String dataset,
			final Consumer<IterableInterval<T>> blockNotFoundHandler,
			final Set<AccessFlags> accessFlags) throws IOException {

		return open(n5, dataset, blockNotFoundHandler, dataType -> new SoftRefLoaderCache<>(), accessFlags);
	}

	/**
	 * Open an N5 dataset as a memory cached {@link LazyCellImg} with a bound on
	 * the number of cache entries.
	 *
	 * @param <T> the type
	 * @param n5 n5 reader
	 * @param dataset the dataset path
	 * @param blockNotFoundHandler consumer handling missing blocks
	 * @param maxNumCacheEntries the maximum number of cache entries
	 * @return the image
	 * @throws IOException the exception
	 */
	public static final <T extends NativeType<T>> CachedCellImg<T, ?> openWithBoundedSoftRefCache(
			final N5Reader n5,
			final String dataset,
			final Consumer<IterableInterval<T>> blockNotFoundHandler,
			final int maxNumCacheEntries) throws IOException {

		return openWithBoundedSoftRefCache(n5, dataset, blockNotFoundHandler, maxNumCacheEntries, AccessFlags.setOf());
	}

	/**
	 * Open an N5 dataset as a memory cached {@link LazyCellImg} with a bound on
	 * the number of cache entries.
	 *
	 * @param <T> the type
	 * @param n5 n5 reader
	 * @param dataset the dataset path
	 * @param blockNotFoundHandler consumer handling missing blocks
	 * @param maxNumCacheEntries the maximum number of cache entries
	 * @param accessFlags the access flag set
	 * @return the image
	 * @throws IOException the exception
	 */
	public static final <T extends NativeType<T>> CachedCellImg<T, ?> openWithBoundedSoftRefCache(
			final N5Reader n5,
			final String dataset,
			final Consumer<IterableInterval<T>> blockNotFoundHandler,
			final int maxNumCacheEntries,
			final Set<AccessFlags> accessFlags) throws IOException {

		return open(n5, dataset, blockNotFoundHandler, dataType -> new BoundedSoftRefLoaderCache<>(maxNumCacheEntries), accessFlags);
	}

	/**
	 * Open an N5 dataset as a memory cached {@link LazyCellImg}.
	 *
	 * @param <T> the type
	 * @param n5 n5 reader
	 * @param dataset the dataset path
	 * @param blockNotFoundHandler consumer handling missing blocks
	 * @param loaderCacheFactory the cache factory
	 * @param accessFlags the access flag set
	 * @return the image
	 * @throws IOException the exception
	 */
	@SuppressWarnings({"unchecked", "rawtypes"})
	public static final <T extends NativeType<T>> CachedCellImg<T, ?> open(
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
	 * @param <T> the voxel type
	 * @param <A> the access type
	 * @param n5 n5 reader
	 * @param dataset the dataset path
	 * @param blockNotFoundHandler consumer handling missing blocks
	 * @param loaderCache the cache
	 * @param accessFlags the access flag set
	 * @param type the type
	 * @return the image
	 * @throws IOException the exception
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
		final CellGrid grid = new CellGrid(dimensions, blockSize);
		final CacheLoader<Long, Cell<A>> loader = new N5CacheLoader<>(n5, dataset, grid, type, accessFlags, blockNotFoundHandler);
		final Cache<Long, Cell<A>> cache = loaderCache.withLoader(loader);
		final CachedCellImg<T, A> img = new CachedCellImg<>(grid, type, cache, ArrayDataAccessFactory.get(type, accessFlags));
		return img;
	}

	/**
	 * Open an N5 dataset as a memory cached {@link LazyCellImg} using
	 * {@link VolatileAccess}.
	 *
	 * @param <T> the type
	 * @param n5 n5 reader
	 * @param dataset the dataset path
	 * @param blockNotFoundHandler consumer handling missing blocks
	 * @return the image
	 * @throws IOException the exception
	 */
	public static final <T extends NativeType<T>> CachedCellImg<T, ?> openVolatile(
			final N5Reader n5,
			final String dataset,
			final Consumer<IterableInterval<T>> blockNotFoundHandler) throws IOException {

		return open(n5, dataset, blockNotFoundHandler, AccessFlags.setOf(AccessFlags.VOLATILE));
	}

	/**
	 * Open an N5 dataset as a memory cached {@link LazyCellImg} with a bound on
	 * the number of cache entries using {@link VolatileAccess}.
	 *
	 * @param <T> the type
	 * @param n5 n5 reader
	 * @param dataset the dataset path
	 * @param blockNotFoundHandler consumer handling missing blocks
	 * @param maxNumCacheEntries the maximum number of cache entries
	 * @return the image
	 * @throws IOException the exception
	 */
	public static <T extends NativeType<T>> CachedCellImg<T, ?> openVolatileWithBoundedSoftRefCache(
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
	 * @param <T> the type parameter
	 * @param n5 the exception
	 * @param group the group path
	 * @param useVolatileAccess uses volatile access if true
	 * @param blockNotFoundHandlerSupplier supply a consumer handling missing blocks
	 * @return the mipmap level images and their respective relative resolutions
	 * @throws IOException the exception
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
	 * @param <T> the type parameter
	 * @param n5 n5 reader
	 * @param group the group path
	 * @param useVolatileAccess uses volatile access if true
	 * @param defaultValueSupplier supplies a default value
	 * @return the mipmap level images and their respective relative resolutions
	 * @throws IOException the exception
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
	 * @param <T> the type parameter
	 * @param n5 n5 reader
	 * @param group the group path
	 * @param useVolatileAccess uses volatile access if true
	 * @return the mipmap level images and their respective relative resolutions
	 * @throws IOException the exception
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
	 * @param <T> the type parameter
	 * @param n5 n5 reader
	 * @param dataset the dataset path
	 * @param blockNotFoundHandler consumer handling missing blocks
	 * @return the image
	 * @throws IOException the exception
	 */
	public static final <T extends NativeType<T>> CachedCellImg<T, ?> openWithDiskCache(
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
	 * source is assumed to align with the {@link DataBlock} grid of the
	 * dataset.
	 *
	 * @param <T> the type parameter
	 * @param source the source image
	 * @param n5 the n5 writer
	 * @param dataset the dataset path 
	 * @param attributes the dataset attributes
	 * @param gridOffset the offset of the source in the larger dataset
	 * @throws IOException the exception
	 */
	public static final <T extends NativeType<T>> void saveBlock(
			RandomAccessibleInterval<T> source,
			final N5Writer n5,
			final String dataset,
			final DatasetAttributes attributes,
			final long[] gridOffset) throws IOException {

		if (N5LabelMultisets.isLabelMultisetType(n5, dataset)) {
			@SuppressWarnings("unchecked")
			final RandomAccessibleInterval<LabelMultisetType> labelMultisetSource = (RandomAccessibleInterval<LabelMultisetType>)source;
			N5LabelMultisets.saveLabelMultisetBlock(labelMultisetSource, n5, dataset, attributes, gridOffset);
			return;
		}

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
	 * Save a {@link RandomAccessibleInterval} into an N5 dataset. The block
	 * offset is determined by the source position, and the source is assumed to
	 * align with the {@link DataBlock} grid of the dataset.
	 *
	 * @param <T> the type parameter
	 * @param source the source image
	 * @param n5 the n5 writer 
	 * @param dataset the dataset path
	 * @param attributes the dataset attributes
	 * @throws IOException the exception
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
	 * Save a {@link RandomAccessibleInterval} into an N5 dataset. The block
	 * offset is determined by the source position, and the source is assumed to
	 * align with the {@link DataBlock} grid of the dataset.
	 *
	 * @param <T> the type parameter
	 * @param source the image to write
	 * @param n5 the n5 writer
	 * @param dataset the dataset path
	 * @throws IOException the exception
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
	 * source is assumed to align with the {@link DataBlock} grid of the
	 * dataset.
	 *
	 * @param <T> the type parameter
	 * @param source the source block
	 * @param n5 the n5 writer
	 * @param dataset the dataset path
	 * @param gridOffset the position in the block grid
	 * @throws IOException the exception
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
	 * @param <T> the type parameter
	 * @param source the source block
	 * @param n5 the n5 writer
	 * @param dataset the dataset path
	 * @param gridOffset the position in the block grid
	 * @param exec the executor service
	 * @throws IOException the io exception
	 * @throws InterruptedException the interrupted exception
	 * @throws ExecutionException the execution exception
	 */
	public static final <T extends NativeType<T>> void saveBlock(
			final RandomAccessibleInterval<T> source,
			final N5Writer n5,
			final String dataset,
			final long[] gridOffset,
			final ExecutorService exec) throws IOException, InterruptedException, ExecutionException {

		if (N5LabelMultisets.isLabelMultisetType(n5, dataset)) {
			@SuppressWarnings("unchecked")
			final RandomAccessibleInterval<LabelMultisetType> labelMultisetSource = (RandomAccessibleInterval<LabelMultisetType>)source;
			N5LabelMultisets.saveLabelMultisetBlock(labelMultisetSource, n5, dataset, gridOffset, exec);
			return;
		}

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

									n5.writeBlock(dataset, attributes, dataBlock);
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
	 * @param <T> the type parameter
	 * @param source the source block
	 * @param n5 the n5 writer
	 * @param dataset the dataset path
	 * @param attributes the dataset attributes
	 * @param gridOffset the position in the block grid
	 * @param defaultValue the default value
	 * @throws IOException the exception
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
	 * Save a {@link RandomAccessibleInterval} into an N5 dataset. The block
	 * offset is determined by the source position, and the source is assumed to
	 * align with the {@link DataBlock} grid of the dataset. Only
	 * {@link DataBlock DataBlocks} that contain values other than a given
	 * default value are stored.
	 *
	 * @param <T> the type parameter
	 * @param source the source block
	 * @param n5 the n5 writer
	 * @param dataset the dataset path
	 * @param attributes the dataset attributes
	 * @param defaultValue the default value
	 * @throws IOException the exception
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
	 * Save a {@link RandomAccessibleInterval} into an N5 dataset. The block
	 * offset is determined by the source position, and the source is assumed to
	 * align with the {@link DataBlock} grid of the dataset. Only
	 * {@link DataBlock DataBlocks} that contain values other than a given
	 * default value are stored.
	 *
	 * @param <T> the type parameter
	 * @param source the source block
	 * @param n5 the n5 writer
	 * @param dataset the dataset path
	 * @param defaultValue the default value
	 * @throws IOException the exception
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
	 * @param <T> the type parameter
	 * @param source the source block
	 * @param n5 the n5 writer
	 * @param dataset the dataset path
	 * @param gridOffset the position in the block grid
	 * @param defaultValue the default value
	 * @throws IOException the exception
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
	 * @param <T> the type parameter
	 * @param source the source image
	 * @param n5 the n5 writer
	 * @param dataset the dataset path
	 * @param blockSize the block size
	 * @param compression the compression type
	 * @throws IOException the exception
	 */
	public static final <T extends NativeType<T>> void save(
			RandomAccessibleInterval<T> source,
			final N5Writer n5,
			final String dataset,
			final int[] blockSize,
			final Compression compression) throws IOException {

		if (Util.getTypeFromInterval(source) instanceof LabelMultisetType) {
			@SuppressWarnings("unchecked")
			final RandomAccessibleInterval<LabelMultisetType> labelMultisetSource = (RandomAccessibleInterval<LabelMultisetType>)source;
			N5LabelMultisets.saveLabelMultiset(labelMultisetSource, n5, dataset, blockSize, compression);
			return;
		}

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
	 * @param <T> the type parameter
	 * @param source the image to write
	 * @param n5 the n5 writer
	 * @param dataset the dataset path
	 * @param blockSize the block size
	 * @param compression the compression type
	 * @param exec executor for parallel writing
	 * @throws IOException the io exception 
	 * @throws InterruptedException the interrupted exception
	 * @throws ExecutionException the execution exception
	 */
	public static final <T extends NativeType<T>> void save(
			final RandomAccessibleInterval<T> source,
			final N5Writer n5,
			final String dataset,
			final int[] blockSize,
			final Compression compression,
			final ExecutorService exec) throws IOException, InterruptedException, ExecutionException {

		if (Util.getTypeFromInterval(source) instanceof LabelMultisetType) {
			@SuppressWarnings("unchecked")
			final RandomAccessibleInterval<LabelMultisetType> labelMultisetSource = (RandomAccessibleInterval<LabelMultisetType>)source;
			N5LabelMultisets.saveLabelMultiset(labelMultisetSource, n5, dataset, blockSize, compression, exec);
			return;
		}

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

								n5.writeBlock(dataset, attributes, dataBlock);
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

	/**
	 * Write an image into an existing n5 dataset, padding the dataset if necessary.
	 * The min and max values of the input source interval define the subset of the
	 * dataset to be written.
	 *
	 * Warning! Avoid calling this method in parallel for multiple sources that have
	 * blocks in common. This risks invalid or corrupting data blocks.
	 *
	 * @param <T> the type parameter
	 * @param source the source image to write
	 * @param n5 the n5 writer
	 * @param dataset the dataset
	 * @throws IOException the io exception
	 * @throws ExecutionException the execution exception
	 * @throws InterruptedException the interrupted exception
	 */
	public static <T extends NativeType<T>> void saveRegion(
			final RandomAccessibleInterval<T> source,
			final N5Writer n5,
			final String dataset ) throws IOException, InterruptedException, ExecutionException
	{
		saveRegion( source, n5, dataset, n5.getDatasetAttributes( dataset ) );
	}

	/**
	 * Write an image into an existing n5 dataset, padding the dataset if necessary.
	 * The min and max values of the input source interval define the subset of the
	 * dataset to be written. Blocks of the output at written in parallel using the given
	 * {@link ExecutorService}.
	 *
	 * Warning! Avoid calling this method in parallel for multiple sources that have
	 * blocks in common. This risks invalid or corrupting data blocks.
	 *
	 * @param <T> the type parameter
	 * @param source the source image to write
	 * @param n5 the n5 writer
	 * @param dataset the dataset
	 * @param exec executor service
	 * @throws IOException the io exception
	 * @throws ExecutionException the execution exception
	 * @throws InterruptedException the interrupted exception
	 */
	public static <T extends NativeType<T>> void saveRegion(
			final RandomAccessibleInterval<T> source,
			final N5Writer n5,
			final String dataset,
			final ExecutorService exec ) throws IOException, InterruptedException, ExecutionException
	{
		saveRegion( source, n5, dataset, n5.getDatasetAttributes( dataset ), exec );
	}

	/**
	 * Write an image into an existing n5 dataset, padding the dataset if necessary.
	 * The min and max values of the input source interval define the subset of the
	 * dataset to be written.
	 *
	 * Warning! Avoid calling this method in parallel for multiple sources that have
	 * blocks in common. This risks invalid or corrupting data blocks.
	 *
	 * @param <T> the type parameter
	 * @param source the source image to write
	 * @param n5 the n5 writer
	 * @param dataset the dataset
	 * @param attributes dataset attributes
	 * @throws IOException the io exception
	 * @throws ExecutionException the execution exception
	 * @throws InterruptedException the interrupted exception
	 */
	public static <T extends NativeType<T>> void saveRegion(
			final RandomAccessibleInterval<T> source,
			final N5Writer n5,
			final String dataset,
			final DatasetAttributes attributes ) throws IOException, InterruptedException, ExecutionException
	{
		Optional< long[] > newDimensionsOpt = saveRegionPreprocessing( source, attributes );

		final long[] dimensions;
		if( newDimensionsOpt.isPresent() )
		{
			n5.setAttribute( dataset, "dimensions", newDimensionsOpt.get() );
			dimensions = newDimensionsOpt.get();
		}
		else
		{
			dimensions = attributes.getDimensions();
		}

		final int n = source.numDimensions();
		final int[] blockSize = attributes.getBlockSize();

		Img< T > currentImg = open( n5, dataset );

		long[] gridOffset = new long[ n ];
		long[] gridMin = new long[ n ];
		long[] gridMax = new long[ n ];
		long[] imgMin = new long[ n ];
		long[] imgMax = new long[ n ];

		// find the grid positions bounding the source image to save
		for (int d = 0; d < n; d++ )
		{
			gridMin[ d ] = Math.floorDiv( source.min( d ), blockSize[ d ] );
			gridMax[ d ] = Math.floorDiv( source.max( d ), blockSize[ d ] );
		}

		// iterate over those blocks
		IntervalIterator it = new IntervalIterator( gridMin, gridMax );
		while( it.hasNext())
		{
			it.fwd();
			it.localize( gridOffset );

			for( int d = 0; d < n; d++ )
			{
				imgMin[ d ] = gridOffset[ d ] * blockSize[ d ];
				imgMax[ d ] = Math.min( imgMin[ d ] + blockSize[ d ] - 1, 
						dimensions[ d ] - 1 );
			}

			//  save the block
			IntervalView< T > currentBlock = Views.interval( currentImg, imgMin, imgMax );
			FinalInterval intersection = Intervals.intersect( currentBlock, source );

			IntervalView< T > srcInt = Views.interval( source, intersection );
			IntervalView< T > blkInt = Views.interval( currentImg, intersection );

			// copy into the part of the block 
			LoopBuilder.setImages( srcInt, blkInt ).forEachPixel( (x,y) -> y.set( x ) );

			N5Utils.saveBlock( currentBlock, n5, dataset, gridOffset );
		}
	}

	/**
	 * Write an image into an existing n5 dataset, padding the dataset if necessary.
	 * The min and max values of the input source interval define the subset of the
	 * dataset to be written. Blocks of the output at written in parallel using the given
	 * {@link ExecutorService}.
	 *
	 * Warning! Avoid calling this method in parallel for multiple sources that have
	 * blocks in common. This risks invalid or corrupting data blocks.
	 *
	 * @param <T> the type parameter
	 * @param source the source image to write
	 * @param n5 the n5 writer
	 * @param dataset the dataset
	 * @param attributes dataset attributes
	 * @param exec the executor
	 * @throws IOException the io exception
	 * @throws ExecutionException the execution exception
	 * @throws InterruptedException the interrupted exception
     *
	 */
	public static <T extends NativeType<T>> void saveRegion(
			final RandomAccessibleInterval<T> source,
			final N5Writer n5,
			final String dataset,
			final DatasetAttributes attributes,
			final ExecutorService exec ) throws InterruptedException, ExecutionException, IOException
	{
		Optional< long[] > newDimensionsOpt = saveRegionPreprocessing( source, attributes );

		final long[] dimensions;
		if( newDimensionsOpt.isPresent() )
		{
			n5.setAttribute( dataset, "dimensions", newDimensionsOpt.get() );
			dimensions = newDimensionsOpt.get();
		}
		else
		{
			dimensions = attributes.getDimensions();
		}

		final int n = source.numDimensions();
		final int[] blockSize = attributes.getBlockSize();

		final Img< T > currentImg = open( n5, dataset );

		final long[] gridOffset = new long[ n ];
		final long[] gridMin = new long[ n ];
		final long[] gridMax = new long[ n ];
		final long[] imgMin = new long[ n ];
		final long[] imgMax = new long[ n ];

		// find the grid positions bounding the source image to save
		for (int d = 0; d < n; d++ )
		{
			gridMin[ d ] = Math.floorDiv( source.min( d ), blockSize[ d ] );
			gridMax[ d ] = Math.floorDiv( source.max( d ), blockSize[ d ] );
		}

		// iterate over those blocks
		final ArrayList<Future<?>> futures = new ArrayList<>();
		IntervalIterator it = new IntervalIterator( gridMin, gridMax );
		while( it.hasNext() )
		{
			it.fwd();
			it.localize( gridOffset );

			for( int d = 0; d < n; d++ )
			{
				imgMin[ d ] = gridOffset[ d ] * blockSize[ d ];
				imgMax[ d ] = Math.min( 
						imgMin[ d ] + blockSize[ d ] - 1, 
						dimensions[ d ] - 1 );
			}

			final long[] imgMinCopy = new long[ n ];
			final long[] imgMaxCopy = new long[ n ];
			final long[] gridOffsetCopy = new long[ n ];

			System.arraycopy( imgMin, 0, imgMinCopy, 0, n );
			System.arraycopy( imgMax, 0, imgMaxCopy, 0, n );
			System.arraycopy( gridOffset, 0, gridOffsetCopy, 0, n );

			futures.add( exec.submit( () ->
			{
				//  save the block
				final IntervalView< T > currentBlock = Views.interval( currentImg, imgMinCopy, imgMaxCopy );
				final FinalInterval intersection = Intervals.intersect( currentBlock, source );

				final IntervalView< T > srcInt = Views.interval( source, intersection );
				final IntervalView< T > blkInt = Views.interval( currentImg, intersection );

				// copy into the part of the block 
				LoopBuilder.setImages( srcInt, blkInt ).forEachPixel( (x,y) -> y.set( x ) );
				try
				{
					N5Utils.saveBlock( currentBlock, n5, dataset, gridOffsetCopy );
				}
				catch ( IOException e )
				{
					e.printStackTrace();
				}

			} ));
		}

		for (final Future<?> f : futures)
			f.get();
	}

	/**
	 * Performs checks, and determine if padding is necessary.
	 * 
	 * @param <T> the type parameter
	 * @param source the source image to write
	 * @param attributes n5 dataset attributes
	 * @return new dataset dimensions if padding necessary, empty optional otherwise
	 */
	private static <T extends NativeType<T>>  Optional< long[] > saveRegionPreprocessing(
			final RandomAccessibleInterval<T> source,
			final DatasetAttributes attributes)
	{
		final DataType dtype = attributes.getDataType();
		final long[] currentDimensions = attributes.getDimensions();
		final int n = currentDimensions.length;

		// ensure source has the correct dimensionality
		if( source.numDimensions() != n )
		{
			throw new ImgLibException( 
					String.format( "Image dimensions (%d) does not match n5 dataset dimensionalidy (%d)",
							source.numDimensions(), n ));
		}

		// ensure type of passed image matches the existing dataset
		final DataType srcType = N5Utils.dataType( Util.getTypeFromInterval( source ));
		if( srcType != dtype )
		{
			throw new ImgLibException( 
					String.format( "Image type (%s) does not match n5 dataset type (%s)",
							srcType, dtype ));
		}

		// check if the volume needs padding
		// and that the source min is >= 0
		boolean needsPadding = false;
		final long[] newDimensions = new long[ n ];

		// set newDimensions to current dimensions
		for( int d = 0; d < n; d++ )
		{
			if( source.min( d ) < 0 )
			{
				throw new ImgLibException( 
						String.format( "Source interval must ",
								source.min( d ), d ));
			}

			if( source.max( d ) + 1 > currentDimensions[ d ] )
			{
				newDimensions[ d ] = source.max( d ) + 1;
				needsPadding = true;
			}
			else
			{
				newDimensions[ d ] = currentDimensions[ d ];
			}
		}

		if( needsPadding )
			return Optional.of( newDimensions );
		else
			return Optional.empty();
	}

	/**
	 * Delete an {@link Interval} in an N5 dataset at a given offset. The offset
	 * is given in {@link DataBlock} grid coordinates and the interval is
	 * assumed to align with the {@link DataBlock} grid of the dataset.
	 *
	 * @param interval the interval
	 * @param n5 the n5 writer 
	 * @param dataset the dataset path
	 * @param attributes dataset attributes
	 * @param gridOffset the position in the block grid
	 * @throws IOException the io exception
	 */
	public static final void deleteBlock(
			final Interval interval,
			final N5Writer n5,
			final String dataset,
			final DatasetAttributes attributes,
			final long[] gridOffset) throws IOException {

		final Interval zeroMinInterval = new FinalInterval(Intervals.dimensionsAsLongArray(interval));
		final int n = zeroMinInterval.numDimensions();
		final long[] max = Intervals.maxAsLongArray(zeroMinInterval);
		final int[] blockSize = attributes.getBlockSize();
		final long[] offset = new long[n];
		final long[] gridPosition = new long[n];
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
			n5.deleteBlock(dataset, gridPosition);
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
	 * Delete an {@link Interval} in an N5 dataset. The block offset is
	 * determined by the interval position, and the interval is assumed to align
	 * with the {@link DataBlock} grid of the dataset.
	 *
	 * @param interval the interval
	 * @param n5 the n5 writer 
	 * @param dataset the dataset path
	 * @param attributes dataset attributes
	 * @throws IOException the io exception
	 */
	public static final void deleteBlock(
			final Interval interval,
			final N5Writer n5,
			final String dataset,
			final DatasetAttributes attributes) throws IOException {

		final int[] blockSize = attributes.getBlockSize();
		final long[] gridOffset = new long[blockSize.length];
		Arrays.setAll(gridOffset, d -> interval.min(d) / blockSize[d]);
		deleteBlock(interval, n5, dataset, attributes, gridOffset);
	}

	/**
	 * Delete an {@link Interval} in an N5 dataset. The block offset is
	 * determined by the interval position, and the interval is assumed to align
	 * with the {@link DataBlock} grid of the dataset.
	 *
	 * @param interval the interval
	 * @param n5 the n5 writer 
	 * @param dataset the dataset path
	 * @throws IOException the io exception
	 */
	public static final void deleteBlock(
			final Interval interval,
			final N5Writer n5,
			final String dataset) throws IOException {

		final DatasetAttributes attributes = n5.getDatasetAttributes(dataset);
		if (attributes != null) {
			deleteBlock(interval, n5, dataset, attributes);
		} else {
			throw new IOException("Dataset " + dataset + " does not exist.");
		}
	}

	/**
	 * Delete an {@link Interval} in an N5 dataset at a given offset. The offset
	 * is given in {@link DataBlock} grid coordinates and the interval is
	 * assumed to align with the {@link DataBlock} grid of the dataset.
	 *
	 * @param interval the interval
	 * @param n5 the n5 writer
	 * @param dataset the dataset path
	 * @param gridOffset the position in the block grid
	 * @throws IOException the io exception
	 */
	public static final void deleteBlock(
			final Interval interval,
			final N5Writer n5,
			final String dataset,
			final long[] gridOffset) throws IOException {

		final DatasetAttributes attributes = n5.getDatasetAttributes(dataset);
		if (attributes != null) {
			deleteBlock(interval, n5, dataset, attributes, gridOffset);
		} else {
			throw new IOException("Dataset " + dataset + " does not exist.");
		}
	}
}

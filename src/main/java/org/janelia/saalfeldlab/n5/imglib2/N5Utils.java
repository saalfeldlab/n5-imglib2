/**
 * Copyright (c) 2017-2019, Stephan Saalfeld, Philipp Hanslovsky, Igor Pisarev
 * John Bogovic
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
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntFunction;

import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5Writer;

import net.imglib2.IterableInterval;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.cache.LoaderCache;
import net.imglib2.cache.img.CachedCellImg;
import net.imglib2.img.basictypeaccess.AccessFlags;
import net.imglib2.img.basictypeaccess.array.ArrayDataAccess;
import net.imglib2.img.basictypeaccess.volatiles.VolatileAccess;
import net.imglib2.img.cell.Cell;
import net.imglib2.img.cell.LazyCellImg;
import net.imglib2.type.NativeType;
import net.imglib2.type.label.LabelMultisetType;
import net.imglib2.util.Pair;

/**
 * Static utility methods to open N5 datasets as ImgLib2
 * {@link RandomAccessibleInterval RandomAccessibleIntervals} and to save
 * ImgLib2 {@link RandomAccessibleInterval RandomAccessibleIntervals} as
 * [sparse] N5 datasets.
 *
 * @author Stephan Saalfeld &lt;saalfelds@janelia.hhmi.org&gt;
 * @author Philipp Hanslovsky &lt;hanslovskyp@janelia.hhmi.org&gt;
 *
 * @deprecated use {@link #N5} instead
 */
@Deprecated
public interface N5Utils {

	public static <T extends NativeType<T>> DataType dataType(final T type) {

		return N5.dataType(type);
	}

	@SuppressWarnings("unchecked")
	public static <T extends NativeType<T>> T type(final DataType dataType) {

		return N5.type(dataType);
	}


	/**
	 * Open an N5 dataset as a memory cached {@link LazyCellImg}.
	 * Supports all primitive types and {@link LabelMultisetType}.
	 *
	 * @param n5
	 * @param dataset
	 * @return
	 * @throws IOException
	 */
	@SuppressWarnings("unchecked")
	public static <T extends NativeType<T>> CachedCellImg<T, ?> open(
			final N5Reader n5,
			final String dataset) throws IOException {

		return N5.open(n5, dataset);
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
	public static <T extends NativeType<T>> CachedCellImg<T, ?> openWithBoundedSoftRefCache(
			final N5Reader n5,
			final String dataset,
			final int maxNumCacheEntries) throws IOException
	{
		return N5.openWithBoundedSoftRefCache(n5, dataset, maxNumCacheEntries);
	}

	/**
	 * Open an N5 dataset as a memory cached {@link LazyCellImg} using
	 * {@link VolatileAccess}.
	 * Supports all primitive types and {@link LabelMultisetType}.
	 *
	 * @param n5
	 * @param dataset
	 * @return
	 * @throws IOException
	 */
	@SuppressWarnings("unchecked")
	public static <T extends NativeType<T>> CachedCellImg<T, ?> openVolatile(
			final N5Reader n5,
			final String dataset) throws IOException {

		return N5.openVolatile(n5, dataset);
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
	public static <T extends NativeType<T>> CachedCellImg<T, ?> openVolatileWithBoundedSoftRefCache(
			final N5Reader n5,
			final String dataset,
			final int maxNumCacheEntries) throws IOException
	{
		return N5.openVolatileWithBoundedSoftRefCache(n5, dataset, maxNumCacheEntries);
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
	public static <T extends NativeType<T>> CachedCellImg<T, ?> openWithDiskCache(
			final N5Reader n5,
			final String dataset) throws IOException {

		return N5.openWithDiskCache(n5, dataset);
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
	public static <T extends NativeType<T>> CachedCellImg<T, ?> open(
			final N5Reader n5,
			final String dataset,
			final T defaultValue) throws IOException {

		return N5.open(n5, dataset, defaultValue);
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
	public static <T extends NativeType<T>> CachedCellImg<T, ?> openWithBoundedSoftRefCache(
			final N5Reader n5,
			final String dataset,
			final int maxNumCacheEntries,
			final T defaultValue) throws IOException {

		return N5.openWithBoundedSoftRefCache(n5, dataset, maxNumCacheEntries, defaultValue);
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
	public static <T extends NativeType<T>> CachedCellImg<T, ?> openVolatile(
			final N5Reader n5,
			final String dataset,
			final T defaultValue) throws IOException {

		return N5.openVolatile(n5, dataset, defaultValue);
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
	public static <T extends NativeType<T>> CachedCellImg<T, ?> openVolatileWithBoundedSoftRefCache(
			final N5Reader n5,
			final String dataset,
			final int maxNumCacheEntries,
			final T defaultValue) throws IOException {

		return N5.openVolatileWithBoundedSoftRefCache(n5, dataset, maxNumCacheEntries, defaultValue);
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
	public static <T extends NativeType<T>> CachedCellImg<T, ?> openWithDiskCache(
			final N5Reader n5,
			final String dataset,
			final T defaultValue) throws IOException {

		return N5.openWithDiskCache(n5, dataset, defaultValue);
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
	public static <T extends NativeType<T>> CachedCellImg<T, ?> open(
			final N5Reader n5,
			final String dataset,
			final Consumer<IterableInterval<T>> blockNotFoundHandler) throws IOException {

		return N5.open(n5, dataset, blockNotFoundHandler);
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
	public static <T extends NativeType<T>> CachedCellImg<T, ?> open(
			final N5Reader n5,
			final String dataset,
			final Consumer<IterableInterval<T>> blockNotFoundHandler,
			final Set<AccessFlags> accessFlags) throws IOException {

		return N5.open(n5, dataset, blockNotFoundHandler, accessFlags);
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
	public static <T extends NativeType<T>> CachedCellImg<T, ?> openWithBoundedSoftRefCache(
			final N5Reader n5,
			final String dataset,
			final Consumer<IterableInterval<T>> blockNotFoundHandler,
			final int maxNumCacheEntries) throws IOException {

		return N5.openWithBoundedSoftRefCache(n5, dataset, blockNotFoundHandler, maxNumCacheEntries);
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
	public static <T extends NativeType<T>> CachedCellImg<T, ?> openWithBoundedSoftRefCache(
			final N5Reader n5,
			final String dataset,
			final Consumer<IterableInterval<T>> blockNotFoundHandler,
			final int maxNumCacheEntries,
			final Set<AccessFlags> accessFlags) throws IOException {

		return N5.openWithBoundedSoftRefCache(n5, dataset, blockNotFoundHandler, maxNumCacheEntries, accessFlags);
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
	public static <T extends NativeType<T>> CachedCellImg<T, ?> open(
			final N5Reader n5,
			final String dataset,
			final Consumer<IterableInterval<T>> blockNotFoundHandler,
			final Function<DataType, LoaderCache> loaderCacheFactory,
			final Set<AccessFlags> accessFlags) throws IOException {

		return N5.open(n5, dataset, blockNotFoundHandler, loaderCacheFactory, accessFlags);
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
	public static <T extends NativeType<T>, A extends ArrayDataAccess<A>> CachedCellImg<T, A> open(
			final N5Reader n5,
			final String dataset,
			final Consumer<IterableInterval<T>> blockNotFoundHandler,
			final LoaderCache<Long, Cell<A>> loaderCache,
			final Set<AccessFlags> accessFlags,
			final T type) throws IOException {

		return N5.open(n5, dataset, blockNotFoundHandler, loaderCache, accessFlags, type);
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
	public static <T extends NativeType<T>> CachedCellImg<T, ?> openVolatile(
			final N5Reader n5,
			final String dataset,
			final Consumer<IterableInterval<T>> blockNotFoundHandler) throws IOException {

		return N5.openVolatile(n5, dataset, blockNotFoundHandler);
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
	public static <T extends NativeType<T>> CachedCellImg<T, ?> openVolatileWithBoundedSoftRefCache(
			final N5Reader n5,
			final String dataset,
			final Consumer<IterableInterval<T>> blockNotFoundHandler,
			final int maxNumCacheEntries) throws IOException {

		return N5.openVolatileWithBoundedSoftRefCache(n5, dataset, blockNotFoundHandler, maxNumCacheEntries);
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
	public static <T extends NativeType<T>> Pair<RandomAccessibleInterval<T>[], double[][]> openMipmapsWithHandler(
			final N5Reader n5,
			final String group,
			final boolean useVolatileAccess,
			final IntFunction<Consumer<IterableInterval<T>>> blockNotFoundHandlerSupplier) throws IOException {

		return N5.openMipmapsWithHandler(n5, group, useVolatileAccess, blockNotFoundHandlerSupplier);
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
	public static <T extends NativeType<T>> Pair<RandomAccessibleInterval<T>[], double[][]> openMipmaps(
			final N5Reader n5,
			final String group,
			final boolean useVolatileAccess,
			final IntFunction<T> defaultValueSupplier) throws IOException {

		return N5.openMipmaps(n5, group, useVolatileAccess, defaultValueSupplier);
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
	public static <T extends NativeType<T>> Pair<RandomAccessibleInterval<T>[], double[][]> openMipmaps(
			final N5Reader n5,
			final String group,
			final boolean useVolatileAccess) throws IOException {

		return N5.openMipmaps(n5, group, useVolatileAccess);
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
	public static <T extends NativeType<T>> CachedCellImg<T, ?> openWithDiskCache(
			final N5Reader n5,
			final String dataset,
			final Consumer<IterableInterval<T>> blockNotFoundHandler) throws IOException {

		return N5.openWithDiskCache(n5, dataset, blockNotFoundHandler);
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
	public static <T extends NativeType<T>> void saveBlock(
			final RandomAccessibleInterval<T> source,
			final N5Writer n5,
			final String dataset,
			final DatasetAttributes attributes,
			final long[] gridOffset) throws IOException {

		N5.saveBlock(source, n5, dataset, attributes, gridOffset);
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
	public static <T extends NativeType<T>> void saveBlock(
			final RandomAccessibleInterval<T> source,
			final N5Writer n5,
			final String dataset,
			final DatasetAttributes attributes) throws IOException {

		N5.saveBlock(source, n5, dataset, attributes);
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
	public static <T extends NativeType<T>> void saveBlock(
			final RandomAccessibleInterval<T> source,
			final N5Writer n5,
			final String dataset) throws IOException {

		N5.saveBlock(source, n5, dataset);
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
	public static <T extends NativeType<T>> void saveBlock(
			final RandomAccessibleInterval<T> source,
			final N5Writer n5,
			final String dataset,
			final long[] gridOffset) throws IOException {

		N5.saveBlock(source, n5, dataset, gridOffset);
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
	public static <T extends NativeType<T>> void saveBlock(
			final RandomAccessibleInterval<T> source,
			final N5Writer n5,
			final String dataset,
			final long[] gridOffset,
			final ExecutorService exec) throws IOException, InterruptedException, ExecutionException {

		N5.saveBlock(source, n5, dataset, gridOffset, exec);
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
	public static <T extends NativeType<T>> void saveNonEmptyBlock(
			final RandomAccessibleInterval<T> source,
			final N5Writer n5,
			final String dataset,
			final DatasetAttributes attributes,
			final long[] gridOffset,
			final T defaultValue) throws IOException {

		N5.saveNonEmptyBlock(source, n5, dataset, attributes, gridOffset, defaultValue);
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
	public static <T extends NativeType<T>> void saveNonEmptyBlock(
			final RandomAccessibleInterval<T> source,
			final N5Writer n5,
			final String dataset,
			final DatasetAttributes attributes,
			final T defaultValue) throws IOException {

		N5.saveNonEmptyBlock(source, n5, dataset, attributes, defaultValue);
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
	public static <T extends NativeType<T>> void saveNonEmptyBlock(
			final RandomAccessibleInterval<T> source,
			final N5Writer n5,
			final String dataset,
			final T defaultValue) throws IOException {

		N5.saveNonEmptyBlock(source, n5, dataset, defaultValue);
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
	public static <T extends NativeType<T>> void saveNonEmptyBlock(
			final RandomAccessibleInterval<T> source,
			final N5Writer n5,
			final String dataset,
			final long[] gridOffset,
			final T defaultValue) throws IOException {

		N5.saveNonEmptyBlock(source, n5, dataset, gridOffset, defaultValue);
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
	public static <T extends NativeType<T>> void save(
			final RandomAccessibleInterval<T> source,
			final N5Writer n5,
			final String dataset,
			final int[] blockSize,
			final Compression compression) throws IOException {

		N5.save(source, n5, dataset, blockSize, compression);
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
	public static <T extends NativeType<T>> void save(
			final RandomAccessibleInterval<T> source,
			final N5Writer n5,
			final String dataset,
			final int[] blockSize,
			final Compression compression,
			final ExecutorService exec) throws IOException, InterruptedException, ExecutionException {

		N5.save(source, n5, dataset, blockSize, compression, exec);
	}
}

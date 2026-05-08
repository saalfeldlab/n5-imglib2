package org.janelia.saalfeldlab.n5.imglib2;

import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Supplier;
import net.imglib2.Interval;
import net.imglib2.IterableInterval;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.blocks.PrimitiveBlocks;
import net.imglib2.blocks.PrimitiveBlocks.OnFallback;
import net.imglib2.blocks.SubArrayCopy;
import net.imglib2.blocks.TempArray;
import net.imglib2.cache.Cache;
import net.imglib2.cache.CacheLoader;
import net.imglib2.cache.LoaderCache;
import net.imglib2.cache.img.CachedCellImg;
import net.imglib2.cache.img.DiskCachedCellImgFactory;
import net.imglib2.cache.img.DiskCachedCellImgOptions;
import net.imglib2.cache.ref.BoundedSoftRefLoaderCache;
import net.imglib2.cache.ref.SoftRefLoaderCache;
import net.imglib2.exception.ImgLibException;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgFactory;
import net.imglib2.img.basictypeaccess.AccessFlags;
import net.imglib2.img.basictypeaccess.ArrayDataAccessFactory;
import net.imglib2.img.basictypeaccess.array.ArrayDataAccess;
import net.imglib2.img.basictypeaccess.volatiles.VolatileAccess;
import net.imglib2.img.cell.Cell;
import net.imglib2.img.cell.CellGrid;
import net.imglib2.img.cell.LazyCellImg;
import net.imglib2.stream.Streams;
import net.imglib2.type.NativeType;
import net.imglib2.type.PrimitiveType;
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
import net.imglib2.util.Cast;
import net.imglib2.util.CloseableThreadLocal;
import net.imglib2.util.Intervals;
import net.imglib2.util.Pair;
import net.imglib2.util.ValuePair;
import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.N5Writer.DataBlockSupplier;
import org.janelia.saalfeldlab.n5.util.FloatValueParser;

import com.google.gson.JsonElement;

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

	public static <T extends NativeType<T>> DataType dataType(final T type) {

		if (type instanceof DoubleType)
			return DataType.FLOAT64;
		if (type instanceof FloatType)
			return DataType.FLOAT32;
		if (type instanceof LongType)
			return DataType.INT64;
		if (type instanceof UnsignedLongType)
			return DataType.UINT64;
		if (type instanceof IntType)
			return DataType.INT32;
		if (type instanceof UnsignedIntType)
			return DataType.UINT32;
		if (type instanceof ShortType)
			return DataType.INT16;
		if (type instanceof UnsignedShortType)
			return DataType.UINT16;
		if (type instanceof ByteType)
			return DataType.INT8;
		if (type instanceof UnsignedByteType)
			return DataType.UINT8;
		else
			return null;
	}

	@SuppressWarnings("unchecked")
	public static <T extends NativeType<T>> T type(final DataType dataType) {

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

	@SuppressWarnings("unchecked")
	public static <T extends NativeType<T>> T type(final DataType dataType, final JsonElement defaultValue) {

		if (defaultValue == null || defaultValue.isJsonNull())
			return null;

		switch (dataType) {
		case INT8:
			return (T) new ByteType(defaultValue.getAsByte());
		case UINT8:
			return (T) new UnsignedByteType(defaultValue.getAsInt());
		case INT16:
			return (T) new ShortType(defaultValue.getAsShort());
		case UINT16:
			return (T) new UnsignedShortType(defaultValue.getAsInt());
		case INT32:
			return (T) new IntType(defaultValue.getAsInt());
		case UINT32:
			return (T) new UnsignedIntType(defaultValue.getAsInt());
		case INT64:
			return (T) new LongType(defaultValue.getAsLong());
		case UINT64:
			return (T) new UnsignedLongType(defaultValue.getAsLong());
		case FLOAT32:

			try {
				return (T) new FloatType(defaultValue.getAsFloat());
			} catch (Exception e) {}

			try {
				String str = defaultValue.getAsString();
				float val;
				if (str.startsWith("0x"))
					val = FloatValueParser.parseFloat(str);
				else
					val = Float.parseFloat(str);

				return (T) new FloatType(val);
			} catch (Exception e) {
				e.printStackTrace();
				return null;
			}

		case FLOAT64:

			try {
				return (T) new DoubleType(defaultValue.getAsDouble());
			} catch (Exception e) {}

			try {
				String str = defaultValue.getAsString();
				double val;
				if (str.startsWith("0x"))
					val = FloatValueParser.parseDouble(str);
				else
					val = Double.parseDouble(str);

				return (T) new DoubleType(val);
			} catch (Exception e) {
				return null;
			}

		default:
			return null;
		}
	}

	/**
	 * Open an N5 dataset as a memory cached {@link LazyCellImg}. Supports all
	 * primitive types and {@link LabelMultisetType}.
	 *
	 * @param <T>
	 *            the type parameter
	 * @param n5
	 *            the n5 reader
	 * @param dataset
	 *            the dataset path
	 * @return the image
	 */
	@SuppressWarnings("unchecked")
	public static <T extends NativeType<T>> CachedCellImg<T, ?> open(
			final N5Reader n5,
			final String dataset) {

		if (N5LabelMultisets.isLabelMultisetType(n5, dataset))
			return (CachedCellImg<T, ?>) N5LabelMultisets.openLabelMultiset(n5, dataset);
		else
			return open(n5, dataset, (Consumer<IterableInterval<T>>) img -> {});
	}

	/**
	 * Open an N5 dataset as a memory cached {@link LazyCellImg}.
	 *
	 * @param <T>
	 *            the type parameter
	 * @param n5
	 *            the n5 reader
	 * @param dataset
	 *            the dataset path
	 * @param maxNumCacheEntries
	 *            the max number of cache entries
	 * @return the image
	 */
	public static <T extends NativeType<T>> CachedCellImg<T, ?> openWithBoundedSoftRefCache(
			final N5Reader n5,
			final String dataset,
			final int maxNumCacheEntries) {

		return openWithBoundedSoftRefCache(n5, dataset, img -> {}, maxNumCacheEntries);
	}

	/**
	 * Open an N5 dataset as a memory cached {@link LazyCellImg} using
	 * {@link VolatileAccess}. Supports all primitive types and
	 * {@link LabelMultisetType}.
	 *
	 * @param <T>
	 *            the type parameter
	 * @param n5
	 *            the n5 reader
	 * @param dataset
	 *            the dataset path
	 * @return the image
	 */
	@SuppressWarnings("unchecked")
	public static <T extends NativeType<T>> CachedCellImg<T, ?> openVolatile(
			final N5Reader n5,
			final String dataset) {

		if (N5LabelMultisets.isLabelMultisetType(n5, dataset))
			return (CachedCellImg<T, ?>) N5LabelMultisets.openLabelMultiset(n5, dataset);
		else
			return openVolatile(n5, dataset, (Consumer<IterableInterval<T>>) img -> {});
	}

	/**
	 * Open an N5 dataset as a memory cached {@link LazyCellImg} using
	 * {@link VolatileAccess}.
	 *
	 * @param <T>
	 *            the type parameter
	 * @param n5
	 *            n5 reader
	 * @param dataset
	 *            the dataset path
	 * @param maxNumCacheEntries
	 *            the max number of cache entries
	 * @return the image
	 */
	public static <T extends NativeType<T>> CachedCellImg<T, ?> openVolatileWithBoundedSoftRefCache(
			final N5Reader n5,
			final String dataset,
			final int maxNumCacheEntries) {

		return openVolatileWithBoundedSoftRefCache(n5, dataset, (Consumer<IterableInterval<T>>)img -> {}, maxNumCacheEntries);
	}

	/**
	 * Open an N5 dataset as a disk-cached {@link LazyCellImg}. Note that this
	 * requires that all parts of the the N5 dataset that will be accessed fit
	 * into /tmp.
	 *
	 * @param <T>
	 *            the type parameter
	 * @param n5
	 *            n5 reader
	 * @param dataset
	 *            the dataset path
	 * @return the image
	 */
	public static <T extends NativeType<T>> CachedCellImg<T, ?> openWithDiskCache(
			final N5Reader n5,
			final String dataset) {

		return openWithDiskCache(n5, dataset, (Consumer<IterableInterval<T>>) img -> {});
	}

	/**
	 * Open an N5 dataset as a memory cached {@link LazyCellImg}.
	 *
	 * @param <T>
	 *            the type parameter
	 * @param n5
	 *            n5 reader
	 * @param dataset
	 *            the dataset path
	 * @param defaultValue
	 *            the default value
	 * @return the image
	 */
	public static <T extends NativeType<T>> CachedCellImg<T, ?> open(
			final N5Reader n5,
			final String dataset,
			final T defaultValue) {

		return open(n5, dataset, N5CacheLoader.setToDefaultValue(defaultValue));
	}

	/**
	 * Open an N5 dataset as a memory cached {@link LazyCellImg}.
	 *
	 * @param <T>
	 *            the type parameter
	 * @param n5
	 *            n5 reader
	 * @param dataset
	 *            the dataset path
	 * @param defaultValue
	 *            the default value
	 * @param maxNumCacheEntries
	 *            the max number of cache entries
	 * @return the image
	 */
	public static <T extends NativeType<T>> CachedCellImg<T, ?> openWithBoundedSoftRefCache(
			final N5Reader n5,
			final String dataset,
			final int maxNumCacheEntries,
			final T defaultValue) {

		return openWithBoundedSoftRefCache(n5, dataset, N5CacheLoader.setToDefaultValue(defaultValue), maxNumCacheEntries);
	}

	/**
	 * Open an N5 dataset as a memory cached {@link LazyCellImg} using
	 * {@link VolatileAccess}.
	 *
	 * @param <T>
	 *            the type parameter
	 * @param n5
	 *            n5 reader
	 * @param dataset
	 *            the dataset path
	 * @param defaultValue
	 *            the default value
	 * @return the image
	 */
	public static <T extends NativeType<T>> CachedCellImg<T, ?> openVolatile(
			final N5Reader n5,
			final String dataset,
			final T defaultValue) {

		return openVolatile(n5, dataset, N5CacheLoader.setToDefaultValue(defaultValue));
	}

	/**
	 * Open an N5 dataset as a memory cached {@link LazyCellImg} using
	 * {@link VolatileAccess}.
	 *
	 * @param <T>
	 *            the type parameter
	 * @param n5
	 *            n5 reader
	 * @param dataset
	 *            the dataset path
	 * @param defaultValue
	 *            the default value
	 * @param maxNumCacheEntries
	 *            the maximum number of cache entries
	 * @return the image
	 */
	public static <T extends NativeType<T>> CachedCellImg<T, ?> openVolatileWithBoundedSoftRefCache(
			final N5Reader n5,
			final String dataset,
			final int maxNumCacheEntries,
			final T defaultValue) {

		return openVolatileWithBoundedSoftRefCache(n5, dataset, N5CacheLoader.setToDefaultValue(defaultValue), maxNumCacheEntries);
	}

	/**
	 * Open an N5 dataset as a disk-cached {@link LazyCellImg}. Note that this
	 * requires that all parts of the the N5 dataset that will be accessed fit
	 * into /tmp.
	 *
	 * @param <T>
	 *            the type parameter
	 * @param n5
	 *            n5 reader
	 * @param dataset
	 *            the dataset path
	 * @param defaultValue
	 *            the default value
	 * @return the image
	 */
	public static <T extends NativeType<T>> CachedCellImg<T, ?> openWithDiskCache(
			final N5Reader n5,
			final String dataset,
			final T defaultValue) {

		return openWithDiskCache(n5, dataset, N5CacheLoader.setToDefaultValue(defaultValue));
	}

	/**
	 * Open an N5 dataset as a memory cached {@link LazyCellImg}.
	 *
	 * @param <T>
	 *            the type
	 * @param n5
	 *            n5 reader
	 * @param dataset
	 *            the dataset path
	 * @param blockNotFoundHandler
	 *            consumer handling missing blocks
	 * @return the image
	 */
	public static <T extends NativeType<T>> CachedCellImg<T, ?> open(
			final N5Reader n5,
			final String dataset,
			final Consumer<IterableInterval<T>> blockNotFoundHandler) {

		return open(n5, dataset, blockNotFoundHandler, AccessFlags.setOf());
	}

	/**
	 * Open an N5 dataset as a memory cached {@link LazyCellImg}.
	 *
	 * @param <T>
	 *            the type
	 * @param n5
	 *            n5 reader
	 * @param dataset
	 *            the dataset path
	 * @param blockNotFoundHandler
	 *            consumer handling missing blocks
	 * @param accessFlags
	 *            the access flag set
	 * @return the image
	 */
	public static <T extends NativeType<T>> CachedCellImg<T, ?> open(
			final N5Reader n5,
			final String dataset,
			final Consumer<IterableInterval<T>> blockNotFoundHandler,
			final Set<AccessFlags> accessFlags) {

		return open(n5, dataset, blockNotFoundHandler, dataType -> new SoftRefLoaderCache<>(), accessFlags);
	}

	/**
	 * Open an N5 dataset as a memory cached {@link LazyCellImg} with a bound on
	 * the number of cache entries.
	 *
	 * @param <T>
	 *            the type
	 * @param n5
	 *            n5 reader
	 * @param dataset
	 *            the dataset path
	 * @param blockNotFoundHandler
	 *            consumer handling missing blocks
	 * @param maxNumCacheEntries
	 *            the maximum number of cache entries
	 * @return the image
	 */
	public static <T extends NativeType<T>> CachedCellImg<T, ?> openWithBoundedSoftRefCache(
			final N5Reader n5,
			final String dataset,
			final Consumer<IterableInterval<T>> blockNotFoundHandler,
			final int maxNumCacheEntries) {

		return openWithBoundedSoftRefCache(n5, dataset, blockNotFoundHandler, maxNumCacheEntries, AccessFlags.setOf());
	}

	/**
	 * Open an N5 dataset as a memory cached {@link LazyCellImg} with a bound on
	 * the number of cache entries.
	 *
	 * @param <T>
	 *            the type
	 * @param n5
	 *            n5 reader
	 * @param dataset
	 *            the dataset path
	 * @param blockNotFoundHandler
	 *            consumer handling missing blocks
	 * @param maxNumCacheEntries
	 *            the maximum number of cache entries
	 * @param accessFlags
	 *            the access flag set
	 * @return the image
	 */
	public static <T extends NativeType<T>> CachedCellImg<T, ?> openWithBoundedSoftRefCache(
			final N5Reader n5,
			final String dataset,
			final Consumer<IterableInterval<T>> blockNotFoundHandler,
			final int maxNumCacheEntries,
			final Set<AccessFlags> accessFlags) {

		return open(n5, dataset, blockNotFoundHandler, dataType -> new BoundedSoftRefLoaderCache<>(maxNumCacheEntries), accessFlags);
	}

	/**
	 * Open an N5 dataset as a memory cached {@link LazyCellImg}.
	 *
	 * @param <T>
	 *            the type
	 * @param n5
	 *            n5 reader
	 * @param dataset
	 *            the dataset path
	 * @param defaultBlockNotFoundHandler
	 *            consumer handling missing blocks if not specified in DatasetAttributes
	 * @param loaderCacheFactory
	 *            the cache factory
	 * @param accessFlags
	 *            the access flag set
	 * @return the image
	 */
	@SuppressWarnings({"unchecked", "rawtypes"})
	public static <T extends NativeType<T>> CachedCellImg<T, ?> open(
			final N5Reader n5,
			final String dataset,
			final Consumer<IterableInterval<T>> defaultBlockNotFoundHandler,
			final Function<DataType, LoaderCache> loaderCacheFactory,
			final Set<AccessFlags> accessFlags) {

		final DatasetAttributes attributes = n5.getDatasetAttributes(dataset);
		final Consumer<IterableInterval<T>> missingBlockHandler = missingBlockHandler(attributes, defaultBlockNotFoundHandler);
		final LoaderCache loaderCache = loaderCacheFactory.apply(attributes.getDataType());
		final T type = type(attributes.getDataType());
		return type == null
				? null
				: open(n5, dataset, missingBlockHandler, loaderCache, accessFlags, type);
	}

	/**
	 * Open an N5 dataset as a memory cached {@link LazyCellImg}.
	 *
	 * @param <T>
	 *            the voxel type
	 * @param <A>
	 *            the access type
	 * @param n5
	 *            n5 reader
	 * @param dataset
	 *            the dataset path
	 * @param defaultBlockNotFoundHandler
	 *            consumer handling missing blocks if not specified in DatasetAttributes
	 * @param loaderCache
	 *            the cache
	 * @param accessFlags
	 *            the access flag set
	 * @param type
	 *            the type
	 * @return the image
	 */
	public static <T extends NativeType<T>, A extends ArrayDataAccess<A>> CachedCellImg<T, A> open(
			final N5Reader n5,
			final String dataset,
			final Consumer<IterableInterval<T>> defaultBlockNotFoundHandler,
			final LoaderCache<Long, Cell<A>> loaderCache,
			final Set<AccessFlags> accessFlags,
			final T type) {

		final DatasetAttributes attributes = n5.getDatasetAttributes(dataset);
		final long[] dimensions = attributes.getDimensions();
		// the N5CacheLoader calls readChunk, therefore needs chunkSize
		final int[] chunkSize = attributes.getChunkSize();
		final CellGrid grid = new CellGrid(dimensions, chunkSize);
		final Consumer<IterableInterval<T>> blockNotFoundHandler = missingBlockHandler(attributes, defaultBlockNotFoundHandler);
		final CacheLoader<Long, Cell<A>> loader = new N5CacheLoader<>(n5, dataset, grid, type, accessFlags, blockNotFoundHandler);
		final Cache<Long, Cell<A>> cache = loaderCache.withLoader(loader);
		final CachedCellImg<T, A> img = new CachedCellImg<>(grid, type, cache, ArrayDataAccessFactory.get(type, accessFlags));
		return img;
	}

	/**
	 * Returns a handler for missing blocks.
	 * <p>
	 * If the {@link DatasetAttributes} specify a default value, the resulting
	 * consumer will fill blocks with that default value. Otherwise, the given
	 * handler will be returned.
	 *
	 * @param <T>
	 *            the type
	 * @param attributes
	 *            the Dataset Attributes
	 * @param defaultBlockNotFoundHandler
	 *            a default handler for missing blocks
	 *
	 * @return a consumer for missing blocks.
	 */
	private static <T extends NativeType<T>> Consumer<IterableInterval<T>> missingBlockHandler(
			DatasetAttributes attributes,
			final Consumer<IterableInterval<T>> defaultBlockNotFoundHandler) {

		// use the default value specified by attributes if present,
		// otherwise use the passed defaultBlockNotFoundHandler
		final T defaultValue = type(attributes.getDataType(), attributes.getDefaultValue());
		return defaultValue == null
				? defaultBlockNotFoundHandler
				: N5CacheLoader.setToDefaultValue(defaultValue);
	}

	/**
	 * Open an N5 dataset as a memory cached {@link LazyCellImg} using
	 * {@link VolatileAccess}.
	 *
	 * @param <T>
	 *            the type
	 * @param n5
	 *            n5 reader
	 * @param dataset
	 *            the dataset path
	 * @param blockNotFoundHandler
	 *            consumer handling missing blocks
	 * @return the image
	 */
	public static <T extends NativeType<T>> CachedCellImg<T, ?> openVolatile(
			final N5Reader n5,
			final String dataset,
			final Consumer<IterableInterval<T>> blockNotFoundHandler) {

		return open(n5, dataset, blockNotFoundHandler, AccessFlags.setOf(AccessFlags.VOLATILE));
	}

	/**
	 * Open an N5 dataset as a memory cached {@link LazyCellImg} with a bound on
	 * the number of cache entries using {@link VolatileAccess}.
	 *
	 * @param <T>
	 *            the type
	 * @param n5
	 *            n5 reader
	 * @param dataset
	 *            the dataset path
	 * @param blockNotFoundHandler
	 *            consumer handling missing blocks
	 * @param maxNumCacheEntries
	 *            the maximum number of cache entries
	 * @return the image
	 */
	public static <T extends NativeType<T>> CachedCellImg<T, ?> openVolatileWithBoundedSoftRefCache(
			final N5Reader n5,
			final String dataset,
			final Consumer<IterableInterval<T>> blockNotFoundHandler,
			final int maxNumCacheEntries) {

		return openWithBoundedSoftRefCache(n5, dataset, blockNotFoundHandler, maxNumCacheEntries, AccessFlags.setOf(AccessFlags.VOLATILE));
	}

	/**
	 * Open an N5 mipmap (multi-scale) group as memory cached
	 * {@link LazyCellImg}s, optionally backed by {@link VolatileAccess}.
	 *
	 * @param <T>
	 *            the type parameter
	 * @param n5
	 *            the exception
	 * @param group
	 *            the group path
	 * @param useVolatileAccess
	 *            uses volatile access if true
	 * @param blockNotFoundHandlerSupplier
	 *            supply a consumer handling missing blocks
	 * @return the mipmap level images and their respective relative resolutions
	 */
	public static <T extends NativeType<T>> Pair<RandomAccessibleInterval<T>[], double[][]> openMipmapsWithHandler(
			final N5Reader n5,
			final String group,
			final boolean useVolatileAccess,
			final IntFunction<Consumer<IterableInterval<T>>> blockNotFoundHandlerSupplier) {

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
	 * @param <T>
	 *            the type parameter
	 * @param n5
	 *            n5 reader
	 * @param group
	 *            the group path
	 * @param useVolatileAccess
	 *            uses volatile access if true
	 * @param defaultValueSupplier
	 *            supplies a default value
	 * @return the mipmap level images and their respective relative resolutions
	 */
	public static <T extends NativeType<T>> Pair<RandomAccessibleInterval<T>[], double[][]> openMipmaps(
			final N5Reader n5,
			final String group,
			final boolean useVolatileAccess,
			final IntFunction<T> defaultValueSupplier) {

		return openMipmapsWithHandler(
				n5,
				group,
				useVolatileAccess,
				s -> {
					return N5CacheLoader.setToDefaultValue(defaultValueSupplier.apply(s));
				});
	}

	/**
	 * Open an N5 mipmap (multi-scale) group as memory cached
	 * {@link LazyCellImg}s, optionally backed by {@link VolatileAccess}.
	 *
	 * @param <T>
	 *            the type parameter
	 * @param n5
	 *            n5 reader
	 * @param group
	 *            the group path
	 * @param useVolatileAccess
	 *            uses volatile access if true
	 * @return the mipmap level images and their respective relative resolutions
	 */
	public static <T extends NativeType<T>> Pair<RandomAccessibleInterval<T>[], double[][]> openMipmaps(
			final N5Reader n5,
			final String group,
			final boolean useVolatileAccess) {

		return openMipmapsWithHandler(
				n5,
				group,
				useVolatileAccess,
				s -> t -> {});
	}

	/**
	 * Open an N5 dataset as a disk-cached {@link LazyCellImg}. Note that this
	 * requires that all parts of the N5 dataset that will be accessed fit
	 * into /tmp.
	 *
	 * @param <T>
	 *            the type parameter
	 * @param n5
	 *            n5 reader
	 * @param dataset
	 *            the dataset path
	 * @param defaultBlockNotFoundHandler
	 *            consumer handling missing blocks
	 * @return the image
	 */
	public static <T extends NativeType<T>, A extends ArrayDataAccess<A>> CachedCellImg<T, ?> openWithDiskCache(
			final N5Reader n5,
			final String dataset,
			final Consumer<IterableInterval<T>> defaultBlockNotFoundHandler) {

		final DatasetAttributes attributes = n5.getDatasetAttributes(dataset);
		final long[] dimensions = attributes.getDimensions();
		// the N5CacheLoader calls readChunk, therefore needs chunkSize
		final int[] chunkSize = attributes.getChunkSize();

		final CellGrid grid = new CellGrid(dimensions, chunkSize);
		final T type = type(attributes.getDataType());
		final Set<AccessFlags> accessFlags = AccessFlags.setOf(AccessFlags.VOLATILE, AccessFlags.DIRTY);
		final Consumer<IterableInterval<T>> blockNotFoundHandler = missingBlockHandler(attributes, defaultBlockNotFoundHandler);
		final CacheLoader<Long, Cell<A>> loader = new N5CacheLoader<>(n5, dataset, grid, type, accessFlags, blockNotFoundHandler);

		final DiskCachedCellImgOptions options = DiskCachedCellImgOptions
				.options()
				.cellDimensions(chunkSize)
				.dirtyAccesses(true)
				.maxCacheSize(100);

		final DiskCachedCellImgFactory<T> factory = new DiskCachedCellImgFactory<T>(
				type, options);

		return factory.createWithCacheLoader(dimensions, loader);
	}

	/**
	 * Save a {@link RandomAccessibleInterval} into an N5 dataset at a given
	 * offset. The offset is given in {@link DataBlock} grid coordinates and the
	 * source is assumed to align with the {@link DataBlock} grid of the
	 * dataset.
	 *
	 * @param <T>
	 *            the type parameter
	 * @param source
	 *            the source image
	 * @param n5
	 *            the n5 writer
	 * @param dataset
	 *            the dataset path
	 * @param attributes
	 *            the dataset attributes
	 * @param gridOffset
	 *            the offset of the source in the larger dataset
	 */
	public static <T extends NativeType<T>> void saveBlock(
			RandomAccessibleInterval<T> source,
			final N5Writer n5,
			final String dataset,
			final DatasetAttributes attributes,
			final long[] gridOffset) {

		if (N5LabelMultisets.isLabelMultisetType(n5, dataset)) {
			@SuppressWarnings("unchecked")
			final RandomAccessibleInterval<LabelMultisetType> labelMultisetSource = (RandomAccessibleInterval<LabelMultisetType>)source;
			N5LabelMultisets.saveLabelMultisetBlock(labelMultisetSource, n5, dataset, attributes, gridOffset);
			return;
		}

		final ChunkSupplier<T,?> chunkSupplier = new DefaultChunkSupplier<>(attributes, source, gridOffset);
		n5.writeRegion(dataset, attributes, chunkSupplier.regionMin(), chunkSupplier.regionSize(), chunkSupplier, true);
	}

	/**
	 * Save a {@link RandomAccessibleInterval} into an N5 dataset. The block
	 * offset is determined by the source position, and the source is assumed to
	 * align with the {@link DataBlock} grid of the dataset.
	 *
	 * @param <T>
	 *            the type parameter
	 * @param source
	 *            the source image
	 * @param n5
	 *            the n5 writer
	 * @param dataset
	 *            the dataset path
	 * @param attributes
	 *            the dataset attributes
	 */
	public static <T extends NativeType<T>> void saveBlock(
			final RandomAccessibleInterval<T> source,
			final N5Writer n5,
			final String dataset,
			final DatasetAttributes attributes) {

		final int[] chunkSize = attributes.getChunkSize();
		final long[] gridOffset = new long[chunkSize.length];
		Arrays.setAll(gridOffset, d -> source.min(d) / chunkSize[d]);
		saveBlock(source, n5, dataset, attributes, gridOffset);
	}

	/**
	 * Save a {@link RandomAccessibleInterval} into an N5 dataset. The block
	 * offset is determined by the source position, and the source is assumed to
	 * align with the {@link DataBlock} grid of the dataset.
	 *
	 * @param <T>
	 *            the type parameter
	 * @param source
	 *            the image to write
	 * @param n5
	 *            the n5 writer
	 * @param dataset
	 *            the dataset path
	 */
	public static <T extends NativeType<T>> void saveBlock(
			final RandomAccessibleInterval<T> source,
			final N5Writer n5,
			final String dataset) {

		final DatasetAttributes attributes = n5.getDatasetAttributes(dataset);
		if (attributes != null) {
			saveBlock(source, n5, dataset, attributes);
		} else {
			throw new N5IOException("Dataset " + dataset + " does not exist.");
		}
	}

	/**
	 * Save a {@link RandomAccessibleInterval} into an N5 dataset at a given
	 * offset. The offset is given in {@link DataBlock} grid coordinates and the
	 * source is assumed to align with the {@link DataBlock} grid of the
	 * dataset.
	 *
	 * @param <T>
	 *            the type parameter
	 * @param source
	 *            the source block
	 * @param n5
	 *            the n5 writer
	 * @param dataset
	 *            the dataset path
	 * @param gridOffset
	 *            the position in the block grid
	 */
	public static <T extends NativeType<T>> void saveBlock(
			final RandomAccessibleInterval<T> source,
			final N5Writer n5,
			final String dataset,
			final long[] gridOffset) {

		final DatasetAttributes attributes = n5.getDatasetAttributes(dataset);
		if (attributes != null) {
			saveBlock(source, n5, dataset, attributes, gridOffset);
		} else {
			throw new N5IOException("Dataset " + dataset + " does not exist.");
		}
	}

	/**
	 * Save a {@link RandomAccessibleInterval} into an N5 dataset at a given
	 * offset, multi-threaded. The offset is given in {@link DataBlock} grid
	 * coordinates and the source is assumed to align with the {@link DataBlock}
	 * grid of the dataset.
	 *
	 * @param <T>
	 *            the type parameter
	 * @param source
	 *            the source block
	 * @param n5
	 *            the n5 writer
	 * @param dataset
	 *            the dataset path
	 * @param attributes
	 *            the dataset attributes
	 * @param gridOffset
	 *            the position in the block grid
	 * @param exec
	 *            the executor service
	 * @throws InterruptedException
	 *             the interrupted exception
	 * @throws ExecutionException
	 *             the execution exception
	 */
	public static <T extends NativeType<T>> void saveBlock(
			final RandomAccessibleInterval<T> source,
			final N5Writer n5,
			final String dataset,
			final DatasetAttributes attributes,
			final long[] gridOffset,
			final ExecutorService exec) throws InterruptedException, ExecutionException {

		if (N5LabelMultisets.isLabelMultisetType(n5, dataset)) {
			@SuppressWarnings("unchecked")
			final RandomAccessibleInterval<LabelMultisetType> labelMultisetSource = (RandomAccessibleInterval<LabelMultisetType>)source;
			N5LabelMultisets.saveLabelMultisetBlock(labelMultisetSource, n5, dataset, gridOffset, exec);
			return;
		}

		final ChunkSupplier<T,?> chunkSupplier = new DefaultChunkSupplier<>(attributes, source, gridOffset).threadSafe();
		n5.writeRegion(dataset, attributes, chunkSupplier.regionMin(), chunkSupplier.regionSize(), chunkSupplier, true, exec);
	}

	/**
	 * Save a {@link RandomAccessibleInterval} into an N5 dataset at a given
	 * offset, multi-threaded. The offset is given in {@link DataBlock} grid
	 * coordinates and the source is assumed to align with the {@link DataBlock}
	 * grid of the dataset.
	 *
	 * @param <T>
	 *            the type parameter
	 * @param source
	 *            the source block
	 * @param n5
	 *            the n5 writer
	 * @param dataset
	 *            the dataset path
	 * @param gridOffset
	 *            the position in the block grid
	 * @param exec
	 *            the executor service
	 * @throws InterruptedException
	 *             the interrupted exception
	 * @throws ExecutionException
	 *             the execution exception
	 */
	public static <T extends NativeType<T>> void saveBlock(
			final RandomAccessibleInterval<T> source,
			final N5Writer n5,
			final String dataset,
			final long[] gridOffset,
			final ExecutorService exec) throws InterruptedException, ExecutionException {

		final DatasetAttributes attributes = n5.getDatasetAttributes(dataset);
		if (attributes != null) {
			saveBlock(source, n5, dataset, attributes, gridOffset, exec);
		} else {
			throw new N5IOException("Dataset " + dataset + " does not exist.");
		}
	}

	/**
	 * Save a {@link RandomAccessibleInterval} into an N5 dataset at a given
	 * offset. The offset is given in {@link DataBlock} grid coordinates and the
	 * source is assumed to align with the {@link DataBlock} grid of the
	 * dataset. Only {@link DataBlock DataBlocks} that contain values other than
	 * a given default value are stored.
	 *
	 * @param <T>
	 *            the type parameter
	 * @param source
	 *            the source block
	 * @param n5
	 *            the n5 writer
	 * @param dataset
	 *            the dataset path
	 * @param attributes
	 *            the dataset attributes
	 * @param gridOffset
	 *            the position in the block grid
	 * @param defaultValue
	 *            the default value
	 */
	public static <T extends NativeType<T>> void saveNonEmptyBlock(
			final RandomAccessibleInterval<T> source,
			final N5Writer n5,
			final String dataset,
			final DatasetAttributes attributes,
			final long[] gridOffset,
			final T defaultValue) {

		final ChunkSupplier<T,?> chunkSupplier = new NonEmptyChunkSupplier<>(attributes, source, gridOffset, defaultValue);
		n5.writeRegion(dataset, attributes, chunkSupplier.regionMin(), chunkSupplier.regionSize(), chunkSupplier, true);
	}

	/**
	 * Save a {@link RandomAccessibleInterval} into an N5 dataset. The block
	 * offset is determined by the source position, and the source is assumed to
	 * align with the {@link DataBlock} grid of the dataset. Only
	 * {@link DataBlock DataBlocks} that contain values other than a given
	 * default value are stored.
	 *
	 * @param <T>
	 *            the type parameter
	 * @param source
	 *            the source block
	 * @param n5
	 *            the n5 writer
	 * @param dataset
	 *            the dataset path
	 * @param attributes
	 *            the dataset attributes
	 * @param defaultValue
	 *            the default value
	 */
	public static <T extends NativeType<T>> void saveNonEmptyBlock(
			final RandomAccessibleInterval<T> source,
			final N5Writer n5,
			final String dataset,
			final DatasetAttributes attributes,
			final T defaultValue) {

		final int[] chunkSize = attributes.getChunkSize();
		final long[] gridOffset = new long[chunkSize.length];
		Arrays.setAll(gridOffset, d -> source.min(d) / chunkSize[d]);
		saveNonEmptyBlock(source, n5, dataset, attributes, gridOffset, defaultValue);
	}

	/**
	 * Save a {@link RandomAccessibleInterval} into an N5 dataset. The block
	 * offset is determined by the source position, and the source is assumed to
	 * align with the {@link DataBlock} grid of the dataset. Only
	 * {@link DataBlock DataBlocks} that contain values other than a given
	 * default value are stored.
	 *
	 * @param <T>
	 *            the type parameter
	 * @param source
	 *            the source block
	 * @param n5
	 *            the n5 writer
	 * @param dataset
	 *            the dataset path
	 * @param defaultValue
	 *            the default value
	 */
	public static <T extends NativeType<T>> void saveNonEmptyBlock(
			final RandomAccessibleInterval<T> source,
			final N5Writer n5,
			final String dataset,
			final T defaultValue) {

		final DatasetAttributes attributes = n5.getDatasetAttributes(dataset);
		if (attributes != null) {
			saveNonEmptyBlock(source, n5, dataset, attributes, defaultValue);
		} else {
			throw new N5IOException("Dataset " + dataset + " does not exist.");
		}
	}

	/**
	 * Save a {@link RandomAccessibleInterval} into an N5 dataset at a given
	 * offset. The offset is given in {@link DataBlock} grid coordinates and the
	 * source is assumed to align with the {@link DataBlock} grid of the
	 * dataset. Only {@link DataBlock DataBlocks} that contain values other than
	 * a given default value are stored.
	 *
	 * @param <T>
	 *            the type parameter
	 * @param source
	 *            the source block
	 * @param n5
	 *            the n5 writer
	 * @param dataset
	 *            the dataset path
	 * @param gridOffset
	 *            the position in the block grid
	 * @param defaultValue
	 *            the default value
	 */
	public static <T extends NativeType<T>> void saveNonEmptyBlock(
			final RandomAccessibleInterval<T> source,
			final N5Writer n5,
			final String dataset,
			final long[] gridOffset,
			final T defaultValue) {

		final DatasetAttributes attributes = n5.getDatasetAttributes(dataset);
		if (attributes != null) {
			saveNonEmptyBlock(source, n5, dataset, attributes, gridOffset, defaultValue);
		} else {
			throw new N5IOException("Dataset " + dataset + " does not exist.");
		}
	}

	/**
	 * Save a {@link RandomAccessibleInterval} as an N5 dataset.
	 * <p>
	 * Warning: this method will overwrite / invalidate any existing data at the specified location.
	 * Manually check if data exists with {@code n5.datasetExists(dataset)} before calling
	 * to avoid overwriting existing data.
	 *
	 * @param <T>
	 *            the type parameter
	 * @param source
	 *            the source image
	 * @param n5
	 *            the n5 writer
	 * @param dataset
	 *            the dataset path
	 * @param blockSize
	 *            the block size
	 * @param compression
	 *            the compression type
	 */
	public static <T extends NativeType<T>> void save(
			final RandomAccessibleInterval<T> source,
			final N5Writer n5,
			final String dataset,
			final int[] blockSize,
			final Compression compression) {

		if (source.getType() instanceof LabelMultisetType) {
			@SuppressWarnings("unchecked")
			final RandomAccessibleInterval<LabelMultisetType> labelMultisetSource = (RandomAccessibleInterval<LabelMultisetType>)source;
			N5LabelMultisets.saveLabelMultiset(labelMultisetSource, n5, dataset, blockSize, compression);
			return;
		}

		final DatasetAttributes attributes = new DatasetAttributes(
				source.dimensionsAsLongArray(),
				blockSize,
				dataType(source.getType()),
				compression);
		final DatasetAttributes writtenAttributes = n5.createDataset(dataset, attributes);
		saveBlock(source, n5, dataset, writtenAttributes);
	}

	/**
	 * Save a {@link RandomAccessibleInterval} as an N5 dataset, multi-threaded.
	 * <p>
	 * Warning: this method will overwrite / invalidate any existing data at the specified location.
	 * Manually check if data exists with {@code n5.datasetExists(dataset)} before calling
	 * to avoid overwriting existing data.
	 *
	 * @param <T>
	 *            the type parameter
	 * @param source
	 *            the image to write
	 * @param n5
	 *            the n5 writer
	 * @param dataset
	 *            the dataset path
	 * @param blockSize
	 *            the block size
	 * @param compression
	 *            the compression type
	 * @param exec
	 *            executor for parallel writing
	 * @throws InterruptedException
	 *             the interrupted exception
	 * @throws ExecutionException
	 *             the execution exception
	 */
	public static <T extends NativeType<T>> void save(
			final RandomAccessibleInterval<T> source,
			final N5Writer n5,
			final String dataset,
			final int[] blockSize,
			final Compression compression,
			final ExecutorService exec) throws InterruptedException, ExecutionException {

		if (source.getType() instanceof LabelMultisetType) {
			@SuppressWarnings("unchecked")
			final RandomAccessibleInterval<LabelMultisetType> labelMultisetSource = (RandomAccessibleInterval<LabelMultisetType>)source;
			N5LabelMultisets.saveLabelMultiset(labelMultisetSource, n5, dataset, blockSize, compression, exec);
			return;
		}

		final DatasetAttributes attributes = new DatasetAttributes(
				source.dimensionsAsLongArray(),
				blockSize,
				dataType(source.getType()),
				compression);
		DatasetAttributes writtenAttributes = n5.createDataset(dataset, attributes);
		final long[] gridOffset = new long[source.numDimensions()];
		saveBlock(source, n5, dataset, writtenAttributes, gridOffset, exec);
	}

	/**
	 * Write an image into an existing n5 dataset, overwriting any exising data, and padding the dataset if
	 * necessary. The min and max values of the input source interval define the
	 * subset of the dataset to be written.
	 * <p>
	 * Warning! Avoid calling this method in parallel for multiple sources that
	 * have blocks in common. This risks invalid or corrupting data blocks.
	 *
	 * @param <T>
	 *            the type parameter
	 * @param source
	 *            the source image to write
	 * @param n5
	 *            the n5 writer
	 * @param dataset
	 *            the dataset
	 */
	public static <T extends NativeType<T>> void saveRegion(
			final RandomAccessibleInterval<T> source,
			final N5Writer n5,
			final String dataset) {

		saveRegion(source, n5, dataset, n5.getDatasetAttributes(dataset));
	}

	/**
	 * Write an image into an existing n5 dataset, overwriting any exising data, and padding the dataset if
	 * necessary. The min and max values of the input source interval define the
	 * subset of the dataset to be written. Blocks of the output at written in
	 * parallel using the given {@link ExecutorService}.
	 * <p>
	 * Warning! Avoid calling this method in parallel for multiple sources that
	 * have blocks or shards in common. This risks invalid or corrupting data blocks.
	 *
	 * @param <T>
	 *            the type parameter
	 * @param source
	 *            the source image to write
	 * @param n5
	 *            the n5 writer
	 * @param dataset
	 *            the dataset
	 * @param exec
	 *            executor service
	 * @throws ExecutionException
	 *             the execution exception
	 * @throws InterruptedException
	 *             the interrupted exception
	 */
	public static <T extends NativeType<T>> void saveRegion(
			final RandomAccessibleInterval<T> source,
			final N5Writer n5,
			final String dataset,
			final ExecutorService exec) throws InterruptedException, ExecutionException {

		saveRegion(source, n5, dataset, n5.getDatasetAttributes(dataset), exec);
	}

	/**
	 * Write an image into an existing n5 dataset, overwriting any exising data, and padding the dataset if
	 * necessary. The min and max values of the input source interval define the
	 * subset of the dataset to be written.
	 * <p>
	 * Warning! Avoid calling this method in parallel for multiple sources that
	 * have blocks or shards in common. This risks invalid or corrupting data blocks.
	 *
	 * @param <T>
	 *            the type parameter
	 * @param source
	 *            the source image to write
	 * @param n5
	 *            the n5 writer
	 * @param dataset
	 *            the dataset
	 * @param attributes
	 *            dataset attributes
	 */
	public static <T extends NativeType<T>, P> void saveRegion(
			final RandomAccessibleInterval<T> source,
			final N5Writer n5,
			final String dataset,
			DatasetAttributes attributes) {

		final Optional<long[]> newDimensionsOpt = saveRegionPreprocessing(source, attributes);

		if (newDimensionsOpt.isPresent()) {
			// TODO not correct for zarr if mapDatasetAttributes not set. I think we need to create a new DatasetAttributes.
			n5.setAttribute(dataset, "dimensions", newDimensionsOpt.get());
			attributes = n5.getDatasetAttributes(dataset);
		}

		final ChunkSupplier<T, ?> chunkSupplier = new MergeChunkSupplier<>(attributes, source);
		n5.writeRegion(dataset, attributes, chunkSupplier.regionMin(), chunkSupplier.regionSize(), chunkSupplier, false);
	}

	/**
	 * Write an image into an existing n5 dataset, overwriting any exising data, and padding the dataset if
	 * necessary. The min and max values of the input source interval define the
	 * subset of the dataset to be written. Blocks of the output at written in
	 * parallel using the given {@link ExecutorService}.
	 * <p>
	 * Warning! Avoid calling this method in parallel for multiple sources that
	 * have blocks or shards in common. This risks invalid or corrupting data blocks.
	 *
	 * @param <T>
	 *            the type parameter
	 * @param source
	 *            the source image to write
	 * @param n5
	 *            the n5 writer
	 * @param dataset
	 *            the dataset
	 * @param attributes
	 *            dataset attributes
	 * @param exec
	 *            the executor
	 * @throws ExecutionException
	 *             the execution exception
	 * @throws InterruptedException
	 *             the interrupted exception
	 */
	public static <T extends NativeType<T>, P> void saveRegion(
			final RandomAccessibleInterval<T> source,
			final N5Writer n5,
			final String dataset,
			DatasetAttributes attributes,
			final ExecutorService exec) throws InterruptedException, ExecutionException {

		final Optional<long[]> newDimensionsOpt = saveRegionPreprocessing(source, attributes);

		if (newDimensionsOpt.isPresent()) {
			// TODO not correct for zarr if mapDatasetAttributes not set. I think we need to create a new DatasetAttributes.
			n5.setAttribute(dataset, "dimensions", newDimensionsOpt.get());
			attributes = n5.getDatasetAttributes(dataset);
		}

		final ChunkSupplier<T, ?> chunkSupplier = new MergeChunkSupplier<>(attributes, source).threadSafe();
		n5.writeRegion(dataset, attributes, chunkSupplier.regionMin(), chunkSupplier.regionSize(), chunkSupplier, false, exec);
	}

	/**
	 * Performs checks, and determine if padding is necessary.
	 *
	 * @param <T>
	 *            the type parameter
	 * @param source
	 *            the source image to write
	 * @param attributes
	 *            n5 dataset attributes
	 * @return new dataset dimensions if padding necessary, empty optional
	 *         otherwise
	 */
	private static <T extends NativeType<T>> Optional<long[]> saveRegionPreprocessing(
			final RandomAccessibleInterval<T> source,
			final DatasetAttributes attributes) {

		final DataType dtype = attributes.getDataType();
		final long[] currentDimensions = attributes.getDimensions();
		final int n = currentDimensions.length;

		// ensure source has the correct dimensionality
		if (source.numDimensions() != n) {
			throw new ImgLibException(
					String.format("Image dimensions (%d) does not match n5 dataset dimensionalidy (%d)",
							source.numDimensions(), n));
		}

		// ensure type of passed image matches the existing dataset
		final DataType srcType = N5Utils.dataType(source.getType());
		if (srcType != dtype) {
			throw new ImgLibException(
					String.format("Image type (%s) does not match n5 dataset type (%s)",
							srcType, dtype));
		}

		// check if the volume needs padding
		// and that the source min is >= 0
		boolean needsPadding = false;
		final long[] newDimensions = new long[n];

		// set newDimensions to current dimensions
		for (int d = 0; d < n; d++) {
			if (source.min(d) < 0) {
				throw new ImgLibException(
						String.format("Source interval min (%d) in dimension %d must be >= 0",
								source.min(d), d));
			}

			if (source.max(d) + 1 > currentDimensions[d]) {
				newDimensions[d] = source.max(d) + 1;
				needsPadding = true;
			} else {
				newDimensions[d] = currentDimensions[d];
			}
		}

		if (needsPadding)
			return Optional.of(newDimensions);
		else
			return Optional.empty();
	}

	/**
	 * Delete an {@link Interval} in an N5 dataset at a given offset. The offset
	 * is given in {@link DataBlock} grid coordinates and the interval size is assumed to be
	 * an integer multiple of the {@link DataBlock} size of the dataset.
	 *
	 * @param interval
	 *            the interval size to delete in pixel units
	 * @param n5
	 *            the n5 writer
	 * @param dataset
	 *            the dataset path
	 * @param attributes
	 *            dataset attributes
	 * @param gridOffset
	 *            the position in the block grid
	 */
	// TODO: the interval is assumed to be zero-min in this method.
	//       Should we change the argument type to Dimensions to make that more obvious?>
	public static void deleteBlock(
			final Interval interval,
			final N5Writer n5,
			final String dataset,
			final DatasetAttributes attributes,
			final long[] gridOffset) {

		final RandomAccessibleInterval<Interval> gridBlocks = new CellGrid(interval.dimensionsAsLongArray(), attributes.getBlockSize())
				.cellIntervals()
				.view().translate(gridOffset);
		Streams.localizing(gridBlocks)
				.forEach(b -> n5.deleteBlock(dataset, b.positionAsLongArray()));
	}

	/**
	 * Delete an {@link Interval} in an N5 dataset. The block offset is
	 * determined by the interval position, and the interval size is assumed to be
	 * an integer multiple of the {@link DataBlock} size of the dataset.
	 *
	 * @param interval
	 *            the interval size to delete in pixel units and whose min
	 *            determines the block grid offset
	 * @param n5
	 *            the n5 writer
	 * @param dataset
	 *            the dataset path
	 * @param attributes
	 *            dataset attributes
	 */
	public static void deleteBlock(
			final Interval interval,
			final N5Writer n5,
			final String dataset,
			final DatasetAttributes attributes) {

		final int[] blockSize = attributes.getBlockSize();
		final long[] gridOffset = new long[blockSize.length];
		Arrays.setAll(gridOffset, d -> interval.min(d) / blockSize[d]);
		deleteBlock(interval, n5, dataset, attributes, gridOffset);
	}

	/**
	 * Delete an {@link Interval} in an N5 dataset. The block offset is
	 * determined by the interval position, and the interval size is assumed to be
	 * an integer multiple of the {@link DataBlock} size of the dataset.
	 *
	 * @param interval
	 *            the interval size to delete in pixel units
	 * @param n5
	 *            the n5 writer
	 * @param dataset
	 *            the dataset path
	 */
	public static void deleteBlock(
			final Interval interval,
			final N5Writer n5,
			final String dataset) {

		final DatasetAttributes attributes = n5.getDatasetAttributes(dataset);
		if (attributes != null) {
			deleteBlock(interval, n5, dataset, attributes);
		} else {
			throw new N5IOException("Dataset " + dataset + " does not exist.");
		}
	}

	/**
	 * Delete an {@link Interval} in an N5 dataset at a given offset. The offset
	 * is given in {@link DataBlock} grid coordinates and the interval size is
	 * assumed to be an integer multiple of the {@link DataBlock} size
	 * of the dataset.
	 *
	 * @param interval
	 *            the interval size to delete in pixel units
	 * @param n5
	 *            the n5 writer
	 * @param dataset
	 *            the dataset path
	 * @param gridOffset
	 *            the position in the block grid
	 */
	public static void deleteBlock(
			final Interval interval,
			final N5Writer n5,
			final String dataset,
			final long[] gridOffset) {

		final DatasetAttributes attributes = n5.getDatasetAttributes(dataset);
		if (attributes != null) {
			deleteBlock(interval, n5, dataset, attributes, gridOffset);
		} else {
			throw new N5IOException("Dataset " + dataset + " does not exist.");
		}
	}

	// ------------------------------------------------------------------------
	//   DataBlockSupplier implementations
	// ------------------------------------------------------------------------

	private static abstract class ChunkSupplier<T extends NativeType<T>, P> implements DataBlockSupplier<P> {

		abstract ChunkSupplier<T, P> independentCopy();

		private Supplier<ChunkSupplier<T, P>> threadSafeSupplier;

		abstract long[] regionMin();

		abstract long[] regionSize();

		ChunkSupplier<T, P> threadSafe() {
			if (threadSafeSupplier == null)
				threadSafeSupplier = CloseableThreadLocal.withInitial(this::independentCopy)::get;
			return new ChunkSupplier<T, P>() {

				@Override
				ChunkSupplier<T, P> independentCopy() {
					return ChunkSupplier.this.independentCopy().threadSafe();
				}

				@Override
				long[] regionMin() {
					return ChunkSupplier.this.regionMin();
				}

				@Override
				long[] regionSize() {
					return ChunkSupplier.this.regionSize();
				}

				@Override
				public DataBlock<P> get(final long[] gridPos, final DataBlock<P> existingDataBlock) {
					return threadSafeSupplier.get().get(gridPos, existingDataBlock);
				}

				@Override
				ChunkSupplier<T, P> threadSafe() {
					return this;
				}
			};
		}
	}

	private static class NonEmptyChunkSupplier<T extends NativeType<T>, P> extends ChunkSupplier<T, P> {

		private final DefaultChunkSupplier<T, P> chunkSupplier;
		private final T defaultValue;

		NonEmptyChunkSupplier(
				final DatasetAttributes attributes,
				final RandomAccessibleInterval<T> source,
				final long[] gridOffset,
				final T defaultValue) {
			chunkSupplier = new DefaultChunkSupplier<>(attributes, source, gridOffset);
			this.defaultValue = defaultValue;
		}

		private NonEmptyChunkSupplier(final NonEmptyChunkSupplier<T, P> supplier) {
			chunkSupplier = supplier.chunkSupplier.independentCopy();
			defaultValue = supplier.defaultValue;
		}

		@Override
		ChunkSupplier<T, P> independentCopy() {
			return new NonEmptyChunkSupplier<>(this);
		}

		@Override
		long[] regionMin() {
			return chunkSupplier.regionMin();
		}

		@Override
		long[] regionSize() {
			return chunkSupplier.regionSize();
		}

		@Override
		public DataBlock<P> get(final long[] gridPos, final DataBlock<P> existingDataBlock) {
			final DataBlock<P> dataBlock = chunkSupplier.get(gridPos, existingDataBlock);
			return allEqual(defaultValue, dataBlock.getData()) ? null : dataBlock;
		}
	}

	private static class DefaultChunkSupplier<T extends NativeType<T>, P> extends ChunkSupplier<T, P> {

		private final DataType dataType;
		private final int[] chunkSize; // (chunk size of the dataset)
		private final PrimitiveBlocks<T> sourceBlocks;
		private final long[] gridOffset;
		private final long[] regionMin;
		private final long[] regionSize;
		private final long[] currentChunkMin;
		private final int[] currentChunkSize;

		DefaultChunkSupplier(
				final DatasetAttributes attributes,
				final RandomAccessibleInterval<T> source,
				final long[] gridOffset) {
			dataType = attributes.getDataType();

			// use the chunk size
			chunkSize = attributes.getChunkSize();

			sourceBlocks = PrimitiveBlocks.of(source.view().zeroMin(), OnFallback.ACCEPT);
			this.gridOffset = gridOffset;

			final int n = source.numDimensions();
			regionMin = new long[n];
			regionSize = new long[n];
			Arrays.setAll(regionMin, d -> gridOffset[d] * chunkSize[d]);
			source.dimensions(regionSize);

			currentChunkMin = new long[n];
			currentChunkSize = new int[n];
		}

		private DefaultChunkSupplier(final DefaultChunkSupplier<T, P> supplier) {
			dataType = supplier.dataType;
			chunkSize = supplier.chunkSize;
			sourceBlocks = supplier.sourceBlocks.independentCopy();
			gridOffset = supplier.gridOffset;
			regionMin = supplier.regionMin;
			regionSize = supplier.regionSize;
			final int n = chunkSize.length;
			currentChunkMin = new long[n];
			currentChunkSize = new int[n];
		}

		@Override
		DefaultChunkSupplier<T, P > independentCopy() {
			return new DefaultChunkSupplier<>(this);
		}

		@Override
		long[] regionMin() {
			return regionMin;
		}

		@Override
		long[] regionSize() {
			return regionSize;
		}

		@Override
		public DataBlock<P> get(final long[] gridPos, final DataBlock<P> existingDataBlock) {
			Arrays.setAll(currentChunkMin, d -> (gridPos[d] - gridOffset[d]) * chunkSize[d]);
			Arrays.setAll(currentChunkSize, d -> (int) Math.min(chunkSize[d], regionSize[d] - currentChunkMin[d]));
			final DataBlock<P> chunk = Cast.unchecked(dataType.createDataBlock(currentChunkSize, gridPos));
			sourceBlocks.copy(currentChunkMin, chunk.getData(), currentChunkSize);
			return chunk;
		}
	}

	private static class MergeChunkSupplier<T extends NativeType<T>, P> extends ChunkSupplier<T, P> {

		private final DataType dataType;
		private final int[] chunkSize; // (chunk size of the dataset)
		private final long[] datasetSize;

		private final PrimitiveBlocks<T> sourceBlocks;
		private final SubArrayCopy.Typed<P, P> subArrayCopy;
		private final TempArray<P> tempArray;

		private final long[] regionMin;
		private final long[] regionSize;

		private final long[] currentChunkMin;
		private final int[] currentChunkSize;

		private final int[] zeroPos;
		private final long[] intersectionMin;
		private final int[] intersectionSize;
		private final int[] intersectionOffset;

		MergeChunkSupplier(
				final DatasetAttributes attributes,
				final RandomAccessibleInterval<T> source) {
			dataType = attributes.getDataType();
			chunkSize = attributes.getBlockSize();
			datasetSize = attributes.getDimensions();

			sourceBlocks = PrimitiveBlocks.of(source, OnFallback.ACCEPT);
			final PrimitiveType p = source.getType().getNativeTypeFactory().getPrimitiveType();
			subArrayCopy = SubArrayCopy.forPrimitiveType(p);
			tempArray = TempArray.forPrimitiveType(p);

			final int n = source.numDimensions();
			regionMin = new long[n];
			regionSize = new long[n];
			source.min(regionMin);
			source.dimensions(regionSize);

			currentChunkMin = new long[n];
			currentChunkSize = new int[n];
			zeroPos = new int[n];
			intersectionMin = new long[n];
			intersectionSize = new int[n];
			intersectionOffset = new int[n];
		}

		private MergeChunkSupplier(final MergeChunkSupplier<T, P> supplier) {
			dataType = supplier.dataType;
			chunkSize = supplier.chunkSize;
			datasetSize = supplier.datasetSize;

			sourceBlocks = supplier.sourceBlocks.independentCopy();
			subArrayCopy = supplier.subArrayCopy;
			tempArray = supplier.tempArray.newInstance();

			regionMin = supplier.regionMin;
			regionSize = supplier.regionSize;
			final int n = chunkSize.length;
			currentChunkMin = new long[n];
			currentChunkSize = new int[n];
			zeroPos = supplier.zeroPos;
			intersectionMin = new long[n];
			intersectionSize = new int[n];
			intersectionOffset = new int[n];
		}

		@Override
		MergeChunkSupplier<T, P > independentCopy() {
			return new MergeChunkSupplier<>(this);
		}

		@Override
		long[] regionMin() {
			return regionMin;
		}

		@Override
		long[] regionSize() {
			return regionSize;
		}

		@Override
		public DataBlock<P> get(final long[] gridPos, final DataBlock<P> existingChunk) {

			Arrays.setAll(currentChunkMin, d -> gridPos[d] * chunkSize[d]);
			Arrays.setAll(currentChunkSize, d -> (int) Math.min(chunkSize[d], datasetSize[d] - currentChunkMin[d]));

			Arrays.setAll(intersectionMin, d -> Math.max(regionMin[d], currentChunkMin[d]));
			Arrays.setAll(intersectionSize, d -> (int) (Math.min(regionMin[d] + regionSize[d], currentChunkMin[d] + currentChunkSize[d]) - intersectionMin[d]));

			final DataBlock<P> chunk;
			if (Arrays.equals(intersectionSize, currentChunkSize)) {
				// Full overlap: Fill a new DataBlock with source data.
				// (It doesn't matter, whether a block already exists at gridPos, we would override everything anyway.)
				chunk = Cast.unchecked(dataType.createDataBlock(currentChunkSize, gridPos));
				sourceBlocks.copy(currentChunkMin, chunk.getData(), currentChunkSize);
			} else {
				if (existingChunk == null) {
					// There is no existing DataBlock. Create a new one.
					chunk = Cast.unchecked(dataType.createDataBlock(currentChunkSize, gridPos));
				} else {
					// There is an existing DataBlock. Is it large enough?
					// Perhaps it was a truncated border block, and now we
					// expanded the dataset.
					if (Arrays.equals(existingChunk.getSize(), currentChunkSize)) {
						chunk = existingChunk;
					} else {
						// Create a new DataBlock and copy existing data over.
						chunk = Cast.unchecked(dataType.createDataBlock(currentChunkSize, gridPos));
						subArrayCopy.copy(
								existingChunk.getData(), existingChunk.getSize(), zeroPos,
								chunk.getData(), chunk.getSize(), zeroPos, existingChunk.getSize());
					}
				}
				// Copy intersecting portion of source data into the DataBlock
				final P sourceData = tempArray.get((int) Intervals.numElements(intersectionSize));
				sourceBlocks.copy(intersectionMin, sourceData, intersectionSize);
				Arrays.setAll(intersectionOffset, d -> (int) (intersectionMin[d] - currentChunkMin[d]));
				subArrayCopy.copy(
						sourceData, intersectionSize, zeroPos,
						chunk.getData(), chunk.getSize(), intersectionOffset, intersectionSize);
			}
			return chunk;
		}
	}

	/**
	 * @return primitive array with one element corresponding to the given value
	 */
	private static <T extends NativeType<T>> Object extractValue(final T value) {
		final ArrayImg<T, ?> img = new ArrayImgFactory<>(value).create(1);
		img.firstElement().set(value);
		return ((ArrayDataAccess<?>) (img.update(null))).getCurrentStorageArray();
	}

	/**
	 * @return {@code true} if all elements of {@code data} are equal to {@code value}
	 */
	private static <T extends NativeType<T>> boolean allEqual(T value, Object data) {
		final PrimitiveType primitiveType = value.getNativeTypeFactory().getPrimitiveType();
		final Object valueArray = extractValue(value);
		switch (primitiveType) {
		case BOOLEAN: {
			final boolean v = ((boolean[]) valueArray)[0];
			final boolean[] booleans = (boolean[]) data;
			for (int i = 0; i < booleans.length; ++i) {
				if (booleans[i] != v) {
					return false;
				}
			}
			return true;
		}
		case BYTE: {
			final byte v = ((byte[]) valueArray)[0];
			final byte[] bytes = (byte[]) data;
			for (int i = 0; i < bytes.length; ++i) {
				if (bytes[i] != v) {
					return false;
				}
			}
			return true;
		}
		case CHAR: {
			final char v = ((char[]) valueArray)[0];
			final char[] chars = (char[]) data;
			for (int i = 0; i < chars.length; ++i) {
				if (chars[i] != v) {
					return false;
				}
			}
			return true;
		}
		case SHORT: {
			final short v = ((short[]) valueArray)[0];
			final short[] shorts = (short[]) data;
			for (int i = 0; i < shorts.length; ++i) {
				if (shorts[i] != v) {
					return false;
				}
			}
			return true;

		}
		case INT: {
			final int v = ((int[]) valueArray)[0];
			final int[] ints = (int[]) data;
			for (int i = 0; i < ints.length; ++i) {
				if (ints[i] != v) {
					return false;
				}
			}
			return true;

		}
		case LONG: {
			final long v = ((long[]) valueArray)[0];
			final long[] longs = (long[]) data;
			for (int i = 0; i < longs.length; ++i) {
				if (longs[i] != v) {
					return false;
				}
			}
			return true;

		}
		case FLOAT: {
			final float v = ((float[]) valueArray)[0];
			final float[] floats = (float[]) data;
			for (int i = 0; i < floats.length; ++i) {
				if (floats[i] != v) {
					return false;
				}
			}
			return true;

		}
		case DOUBLE: {
			final double v = ((double[]) valueArray)[0];
			final double[] doubles = (double[]) data;
			for (int i = 0; i < doubles.length; ++i) {
				if (doubles[i] != v) {
					return false;
				}
			}
			return true;
		}
		default:
			throw new UnsupportedOperationException();
		}
	}
}

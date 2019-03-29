package org.janelia.saalfeldlab.n5.imglib2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.BiFunction;

import org.janelia.saalfeldlab.n5.ByteArrayDataBlock;
import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5Writer;

import net.imglib2.RandomAccessibleInterval;
import net.imglib2.cache.LoaderCache;
import net.imglib2.cache.img.CachedCellImg;
import net.imglib2.cache.ref.SoftRefLoaderCache;
import net.imglib2.cache.util.LoaderCacheAsCacheAdapter;
import net.imglib2.img.cell.Cell;
import net.imglib2.img.cell.CellGrid;
import net.imglib2.img.cell.LazyCellImg;
import net.imglib2.type.label.Label;
import net.imglib2.type.label.LabelMultisetType;
import net.imglib2.type.label.LabelMultisetType.Entry;
import net.imglib2.type.label.LabelUtils;
import net.imglib2.type.label.VolatileLabelMultisetArray;
import net.imglib2.util.Intervals;
import net.imglib2.view.Views;

public class N5LabelMultisets {

	public static final String LABEL_MULTISETTYPE_KEY = "isLabelMultiset";

	/**
	 * Determine whether an N5 dataset is of type {@link LabelMultisetType}.
	 *
	 * @param n5
	 * @param dataset
	 * @return
	 */
	public static boolean isLabelMultisetType(final N5Reader n5, final String dataset) throws IOException {

		return Optional
				.ofNullable(n5.getAttribute(dataset, LABEL_MULTISETTYPE_KEY, Boolean.class))
				.orElse(false);
	}

	/**
	 * Open an N5 dataset of {@link LabelMultisetType} as a memory cached {@link LazyCellImg}.
	 *
	 * @param n5
	 * @param dataset
	 * @return
	 * @throws IOException
	 */
	public static final RandomAccessibleInterval<LabelMultisetType> openLabelMultiset(
			final N5Reader n5,
			final String dataset) throws IOException {

		return openLabelMultiset(n5, dataset, Label.BACKGROUND);
	}

	/**
	 * Open an N5 dataset of {@link LabelMultisetType} as a memory cached {@link LazyCellImg}.
	 *
	 * @param n5
	 * @param dataset
	 * @param defaultLabelId
	 * @return
	 * @throws IOException
	 */
	public static final RandomAccessibleInterval<LabelMultisetType> openLabelMultiset(
			final N5Reader n5,
			final String dataset,
			final long defaultLabelId) throws IOException {

		return openLabelMultiset(n5, dataset, N5LabelMultisetCacheLoader.constantNullReplacement(defaultLabelId));
	}

	/**
	 * Open an N5 dataset of {@link LabelMultisetType} as a memory cached {@link LazyCellImg}.
	 *
	 * @param n5
	 * @param dataset
	 * @param nullReplacement
	 * @return
	 * @throws IOException
	 */
	public static final RandomAccessibleInterval<LabelMultisetType> openLabelMultiset(
			final N5Reader n5,
			final String dataset,
			final BiFunction<CellGrid, long[], byte[]> nullReplacement) throws IOException {

		return openLabelMultiset(n5, dataset, nullReplacement, new SoftRefLoaderCache<>());
	}

	/**
	 * Open an N5 dataset of {@link LabelMultisetType} as a memory cached {@link LazyCellImg}.
	 *
	 * @param n5
	 * @param dataset
	 * @param nullReplacement
	 * @param loaderCache
	 * @return
	 * @throws IOException
	 */
	public static final CachedCellImg<LabelMultisetType, VolatileLabelMultisetArray> openLabelMultiset(
			final N5Reader n5,
			final String dataset,
			final BiFunction<CellGrid, long[], byte[]> nullReplacement,
			final LoaderCache<Long, Cell<VolatileLabelMultisetArray>> loaderCache) throws IOException {

		if (!isLabelMultisetType(n5, dataset))
			throw new IOException(dataset + " is not a label multiset dataset.");

		final DatasetAttributes attributes = n5.getDatasetAttributes(dataset);
		final CellGrid grid = new CellGrid(attributes.getDimensions(), attributes.getBlockSize());

		final N5LabelMultisetCacheLoader loader = new N5LabelMultisetCacheLoader(n5, dataset, nullReplacement);
		final LoaderCacheAsCacheAdapter<Long, Cell<VolatileLabelMultisetArray>> wrappedCache = new LoaderCacheAsCacheAdapter<>(loaderCache, loader);

		final CachedCellImg<LabelMultisetType, VolatileLabelMultisetArray> cachedImg = new CachedCellImg<>(
				grid,
				new LabelMultisetType().getEntitiesPerPixel(),
				wrappedCache,
				new VolatileLabelMultisetArray(0, true, new long[] {Label.INVALID}));
		cachedImg.setLinkedType(new LabelMultisetType(cachedImg));
		return cachedImg;
	}

	/**
	 * Save a {@link RandomAccessibleInterval} of type {@link LabelMultisetType} as an N5 dataset.
	 *
	 * @param source
	 * @param n5
	 * @param dataset
	 * @param blockSize
	 * @param compression
	 * @throws IOException
	 */
	public static final void saveLabelMultiset(
			RandomAccessibleInterval<LabelMultisetType> source,
			final N5Writer n5,
			final String dataset,
			final int[] blockSize,
			final Compression compression) throws IOException {

		source = Views.zeroMin(source);
		final long[] dimensions = Intervals.dimensionsAsLongArray(source);
		final DatasetAttributes attributes = new DatasetAttributes(
				dimensions,
				blockSize,
				DataType.UINT8,
				compression);

		n5.createDataset(dataset, attributes);
		n5.setAttribute(dataset, LABEL_MULTISETTYPE_KEY, true);

		final int n = dimensions.length;
		final long[] max = Intervals.maxAsLongArray(source);
		final long[] offset = new long[n];
		final long[] gridPosition = new long[n];
		final int[] intCroppedBlockSize = new int[n];
		final long[] longCroppedBlockSize = new long[n];
		for (int d = 0; d < n;) {
			N5Utils.cropBlockDimensions(max, offset, blockSize, longCroppedBlockSize, intCroppedBlockSize, gridPosition);
			final RandomAccessibleInterval<LabelMultisetType> sourceBlock = Views.offsetInterval(source, offset, longCroppedBlockSize);
			final ByteArrayDataBlock dataBlock = createDataBlock(sourceBlock, gridPosition);

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
	 * Save a {@link RandomAccessibleInterval} of type {@link LabelMultisetType} as an N5 dataset, multi-threaded.
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
	public static final void saveLabelMultiset(
			final RandomAccessibleInterval<LabelMultisetType> source,
			final N5Writer n5,
			final String dataset,
			final int[] blockSize,
			final Compression compression,
			final ExecutorService exec) throws IOException, InterruptedException, ExecutionException {

		final RandomAccessibleInterval<LabelMultisetType> zeroMinSource = Views.zeroMin(source);
		final long[] dimensions = Intervals.dimensionsAsLongArray(zeroMinSource);
		final DatasetAttributes attributes = new DatasetAttributes(
				dimensions,
				blockSize,
				DataType.UINT8,
				compression);

		n5.createDataset(dataset, attributes);
		n5.setAttribute(dataset, LABEL_MULTISETTYPE_KEY, true);

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

								N5Utils.cropBlockDimensions(
										max,
										fOffset,
										blockSize,
										longCroppedBlockSize,
										intCroppedBlockSize,
										gridPosition);

								final RandomAccessibleInterval<LabelMultisetType> sourceBlock = Views
										.offsetInterval(zeroMinSource, fOffset, longCroppedBlockSize);
								final ByteArrayDataBlock dataBlock = createDataBlock(sourceBlock, gridPosition);

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

	/**
	 * Save a {@link RandomAccessibleInterval} of type {@link LabelMultisetType} into an existing N5 dataset.
	 *
	 * @param source
	 * @param n5
	 * @param dataset
	 * @param attributes
	 * @param gridOffset
	 * @throws IOException
	 */
	public static final void saveLabelMultisetBlock(
			RandomAccessibleInterval<LabelMultisetType> source,
			final N5Writer n5,
			final String dataset,
			final DatasetAttributes attributes,
			final long[] gridOffset) throws IOException {

		if (!isLabelMultisetType(n5, dataset))
			throw new IOException(dataset + " is not a label multiset dataset.");

		source = Views.zeroMin(source);
		final long[] dimensions = Intervals.dimensionsAsLongArray(source);

		final int n = dimensions.length;
		final long[] max = Intervals.maxAsLongArray(source);
		final long[] offset = new long[n];
		final long[] gridPosition = new long[n];
		final int[] blockSize = attributes.getBlockSize();
		final int[] intCroppedBlockSize = new int[n];
		final long[] longCroppedBlockSize = new long[n];
		for (int d = 0; d < n;) {
			N5Utils.cropBlockDimensions(
					max,
					offset,
					gridOffset,
					blockSize,
					longCroppedBlockSize,
					intCroppedBlockSize,
					gridPosition);
			final RandomAccessibleInterval<LabelMultisetType> sourceBlock = Views.offsetInterval(source, offset, longCroppedBlockSize);
			final ByteArrayDataBlock dataBlock = createDataBlock(sourceBlock, gridPosition);

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
	 * Save a {@link RandomAccessibleInterval} of type {@link LabelMultisetType} into an existing N5 dataset.
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
	public static final void saveLabelMultisetBlock(
			final RandomAccessibleInterval<LabelMultisetType> source,
			final N5Writer n5,
			final String dataset,
			final DatasetAttributes attributes) throws IOException {

		final int[] blockSize = attributes.getBlockSize();
		final long[] gridOffset = new long[blockSize.length];
		Arrays.setAll(gridOffset, d -> source.min(d) / blockSize[d]);
		saveLabelMultisetBlock(source, n5, dataset, attributes, gridOffset);
	}

	/**
	 * Save a {@link RandomAccessibleInterval} of type {@link LabelMultisetType} into an existing N5 dataset.
	 * The block offset is determined by the source position, and the
	 * source is assumed to align with the {@link DataBlock} grid
	 * of the dataset.
	 *
	 * @param source
	 * @param n5
	 * @param dataset
	 * @throws IOException
	 */
	public static final void saveLabelMultisetBlock(
			final RandomAccessibleInterval<LabelMultisetType> source,
			final N5Writer n5,
			final String dataset) throws IOException {

		final DatasetAttributes attributes = n5.getDatasetAttributes(dataset);
		if (attributes != null) {
			saveLabelMultisetBlock(source, n5, dataset, attributes);
		} else {
			throw new IOException("Dataset " + dataset + " does not exist.");
		}
	}

	/**
	 * Save a {@link RandomAccessibleInterval} of type {@link LabelMultisetType} into an existing N5 dataset.
	 *
	 * @param source
	 * @param n5
	 * @param dataset
	 * @param gridOffset
	 * @throws IOException
	 */
	public static final void saveLabelMultisetBlock(
			final RandomAccessibleInterval<LabelMultisetType> source,
			final N5Writer n5,
			final String dataset,
			final long[] gridOffset) throws IOException {

		final DatasetAttributes attributes = n5.getDatasetAttributes(dataset);
		if (attributes != null) {
			saveLabelMultisetBlock(source, n5, dataset, attributes, gridOffset);
		} else {
			throw new IOException("Dataset " + dataset + " does not exist.");
		}
	}

	/**
	 * Save a {@link RandomAccessibleInterval} of type {@link LabelMultisetType} into an existing N5 dataset, multi-threaded.
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
	public static final void saveLabelMultisetBlock(
			final RandomAccessibleInterval<LabelMultisetType> source,
			final N5Writer n5,
			final String dataset,
			final long[] gridOffset,
			final ExecutorService exec) throws IOException, InterruptedException, ExecutionException {

		if (!isLabelMultisetType(n5, dataset))
			throw new IOException(dataset + " is not a label multiset dataset.");

		final RandomAccessibleInterval<LabelMultisetType> zeroMinSource = Views.zeroMin(source);
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

									N5Utils.cropBlockDimensions(
											max,
											fOffset,
											gridOffset,
											blockSize,
											longCroppedBlockSize,
											intCroppedBlockSize,
											gridPosition);

									final RandomAccessibleInterval<LabelMultisetType> sourceBlock = Views
											.offsetInterval(zeroMinSource, fOffset, longCroppedBlockSize);
									final ByteArrayDataBlock dataBlock = createDataBlock(sourceBlock, gridPosition);

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
	 * Save a {@link RandomAccessibleInterval} of type {@link LabelMultisetType} into an N5 dataset at a given
	 * offset. The offset is given in {@link DataBlock} grid coordinates and the
	 * source is assumed to align with the {@link DataBlock} grid of the
	 * dataset. Only {@link DataBlock DataBlocks} that contain labels other than
	 * a given default label are stored.
	 *
	 * @param source
	 * @param n5
	 * @param dataset
	 * @param attributes
	 * @param gridOffset
	 * @param defaultLabelId
	 * @throws IOException
	 */
	public static final void saveLabelMultisetNonEmptyBlock(
			RandomAccessibleInterval<LabelMultisetType> source,
			final N5Writer n5,
			final String dataset,
			final DatasetAttributes attributes,
			final long[] gridOffset,
			final long defaultLabelId) throws IOException {

		if (!isLabelMultisetType(n5, dataset))
			throw new IOException(dataset + " is not a label multiset dataset.");

		source = Views.zeroMin(source);
		final long[] dimensions = Intervals.dimensionsAsLongArray(source);

		final int n = dimensions.length;
		final long[] max = Intervals.maxAsLongArray(source);
		final long[] offset = new long[n];
		final long[] gridPosition = new long[n];
		final int[] blockSize = attributes.getBlockSize();
		final int[] intCroppedBlockSize = new int[n];
		final long[] longCroppedBlockSize = new long[n];
		for (int d = 0; d < n;) {
			N5Utils.cropBlockDimensions(
					max,
					offset,
					gridOffset,
					blockSize,
					longCroppedBlockSize,
					intCroppedBlockSize,
					gridPosition);
			final RandomAccessibleInterval<LabelMultisetType> sourceBlock = Views.offsetInterval(source, offset, longCroppedBlockSize);
			final ByteArrayDataBlock dataBlock = createNonEmptyDataBlock(sourceBlock, gridPosition, defaultLabelId);

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
	 * Save a {@link RandomAccessibleInterval} of type {@link LabelMultisetType} into an N5 dataset.
	 * The block offset is determined by the source position, and the
	 * source is assumed to align with the {@link DataBlock} grid
	 * of the dataset.
	 * Only {@link DataBlock DataBlocks} that contain labels other than
	 * a given default label are stored.
	 *
	 * @param source
	 * @param n5
	 * @param dataset
	 * @param attributes
	 * @param defaultLabelId
	 * @throws IOException
	 */
	public static final void saveLabelMultisetNonEmptyBlock(
			final RandomAccessibleInterval<LabelMultisetType> source,
			final N5Writer n5,
			final String dataset,
			final DatasetAttributes attributes,
			final long defaultLabelId) throws IOException {

		final int[] blockSize = attributes.getBlockSize();
		final long[] gridOffset = new long[blockSize.length];
		Arrays.setAll(gridOffset, d -> source.min(d) / blockSize[d]);
		saveLabelMultisetNonEmptyBlock(source, n5, dataset, attributes, gridOffset, defaultLabelId);
	}

	/**
	 * Save a {@link RandomAccessibleInterval} of type {@link LabelMultisetType} into an N5 dataset.
	 * The block offset is determined by the source position, and the
	 * source is assumed to align with the {@link DataBlock} grid
	 * of the dataset.
	 * Only {@link DataBlock DataBlocks} that contain labels other than
	 * a given default label are stored.
	 *
	 * @param source
	 * @param n5
	 * @param dataset
	 * @param defaultLabelId
	 * @throws IOException
	 */
	public static final void saveLabelMultisetNonEmptyBlock(
			final RandomAccessibleInterval<LabelMultisetType> source,
			final N5Writer n5,
			final String dataset,
			final long defaultLabelId) throws IOException {

		final DatasetAttributes attributes = n5.getDatasetAttributes(dataset);
		if (attributes != null) {
			saveLabelMultisetNonEmptyBlock(source, n5, dataset, attributes, defaultLabelId);
		} else {
			throw new IOException("Dataset " + dataset + " does not exist.");
		}
	}

	/**
	 * Save a {@link RandomAccessibleInterval} of type {@link LabelMultisetType} into an N5 dataset.
	 * The block offset is determined by the source position, and the
	 * source is assumed to align with the {@link DataBlock} grid
	 * of the dataset.
	 * Only {@link DataBlock DataBlocks} that contain labels other than
	 * {@link Label#BACKGROUND} are stored.
	 *
	 * @param source
	 * @param n5
	 * @param dataset
	 * @throws IOException
	 */
	public static final void saveLabelMultisetNonEmptyBlock(
			final RandomAccessibleInterval<LabelMultisetType> source,
			final N5Writer n5,
			final String dataset) throws IOException {

		saveLabelMultisetNonEmptyBlock(source, n5, dataset, Label.BACKGROUND);
	}

	/**
	 * Save a {@link RandomAccessibleInterval} of type {@link LabelMultisetType} into an N5 dataset at a given
	 * offset. The offset is given in {@link DataBlock} grid coordinates and the
	 * source is assumed to align with the {@link DataBlock} grid of the
	 * dataset. Only {@link DataBlock DataBlocks} that contain labels other than
	 * a given default label are stored.
	 *
	 * @param source
	 * @param n5
	 * @param dataset
	 * @param gridOffset
	 * @param defaultLabelId
	 * @throws IOException
	 */
	public static final void saveLabelMultisetNonEmptyBlock(
			final RandomAccessibleInterval<LabelMultisetType> source,
			final N5Writer n5,
			final String dataset,
			final long[] gridOffset,
			final long defaultLabelId) throws IOException {

		final DatasetAttributes attributes = n5.getDatasetAttributes(dataset);
		if (attributes != null) {
			saveLabelMultisetNonEmptyBlock(source, n5, dataset, attributes, gridOffset, defaultLabelId);
		} else {
			throw new IOException("Dataset " + dataset + " does not exist.");
		}
	}

	/**
	 * Save a {@link RandomAccessibleInterval} of type {@link LabelMultisetType} into an N5 dataset at a given
	 * offset. The offset is given in {@link DataBlock} grid coordinates and the
	 * source is assumed to align with the {@link DataBlock} grid of the
	 * dataset. Only {@link DataBlock DataBlocks} that contain labels other than
	 * {@link Label#BACKGROUND} are stored.
	 *
	 * @param source
	 * @param n5
	 * @param dataset
	 * @param gridOffset
	 * @throws IOException
	 */
	public static final void saveLabelMultisetNonEmptyBlock(
			final RandomAccessibleInterval<LabelMultisetType> source,
			final N5Writer n5,
			final String dataset,
			final long[] gridOffset) throws IOException {

		saveLabelMultisetNonEmptyBlock(source, n5, dataset, gridOffset, Label.BACKGROUND);
	}

	/**
	 * Creates a {@link ByteArrayDataBlock} with serialized source contents of type {@link LabelMultisetType}.
	 *
	 * @param source
	 * @param gridPosition
	 * @return
	 */
	private static final ByteArrayDataBlock createDataBlock(
			final RandomAccessibleInterval<LabelMultisetType> source,
			final long[] gridPosition) {

		final byte[] data = LabelUtils.serializeLabelMultisetTypes(
				Views.flatIterable(source),
				(int) Intervals.numElements(source)
			);

		final ByteArrayDataBlock dataBlock = new ByteArrayDataBlock(
				Intervals.dimensionsAsIntArray(source),
				gridPosition,
				data
			);

		return dataBlock;
	}

	/**
	 * Creates a {@link ByteArrayDataBlock} with serialized source contents of type {@link LabelMultisetType},
	 * or returns {@code null} if all labels are equal to {@code defaultLabelId} (regardless of their counts).
	 *
	 * @param source
	 * @param gridPosition
	 * @param defaultLabelId
	 * @return
	 */
	private static final ByteArrayDataBlock createNonEmptyDataBlock(
			final RandomAccessibleInterval<LabelMultisetType> source,
			final long[] gridPosition,
			final long defaultLabelId) {

		boolean isEmpty = true;
		for (final LabelMultisetType lmt : Views.iterable(source))
			for (final Entry<Label> entry : lmt.entrySet())
				isEmpty &= entry.getElement().id() == defaultLabelId;

		return isEmpty ? null : createDataBlock(source, gridPosition);
	}
}

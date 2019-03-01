package org.janelia.saalfeldlab.n5.imglib2;

import java.io.IOException;
import java.util.Optional;
import java.util.function.BiFunction;

import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5Reader;

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
import net.imglib2.type.label.VolatileLabelMultisetArray;

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
}

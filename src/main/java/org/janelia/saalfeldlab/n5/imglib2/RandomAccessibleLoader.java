package org.janelia.saalfeldlab.n5.imglib2;

import net.imglib2.Cursor;
import net.imglib2.RandomAccessible;
import net.imglib2.cache.img.CellLoader;
import net.imglib2.cache.img.SingleCellArrayImg;
import net.imglib2.type.NativeType;
import net.imglib2.view.Views;

/**
 * A {@link CellLoader} that copies its data from a {@link RandomAccessible}.
 *
 * @deprecated This class belongs into imglib2-cache.
 *
 * @param <T>
 *            the type parameter
 *
 * @author Philipp Hanslovsky
 * @author Stephan Saalfeld
 */
@Deprecated
public class RandomAccessibleLoader<T extends NativeType<T>> implements CellLoader<T> {

	private final RandomAccessible<T> source;

	public RandomAccessibleLoader(final RandomAccessible<T> source) {

		super();
		this.source = source;
	}

	@Override
	public void load(final SingleCellArrayImg<T, ?> cell) {

		for (Cursor<T> s = Views.flatIterable(Views.interval(source, cell)).cursor(), t = cell.cursor(); s.hasNext();)
			t.next().set(s.next());
	}
}

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
import java.util.Arrays;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5Reader;

import net.imglib2.Cursor;
import net.imglib2.FinalInterval;
import net.imglib2.Interval;
import net.imglib2.IterableInterval;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.cache.img.CellLoader;
import net.imglib2.cache.img.SingleCellArrayImg;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.type.NativeType;
import net.imglib2.type.Type;
import net.imglib2.type.numeric.integer.GenericByteType;
import net.imglib2.type.numeric.integer.GenericIntType;
import net.imglib2.type.numeric.integer.GenericLongType;
import net.imglib2.type.numeric.integer.GenericShortType;
import net.imglib2.type.numeric.real.DoubleType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.Intervals;
import net.imglib2.view.IntervalView;
import net.imglib2.view.Views;

/**
 * A {@link CellLoader} for N5 dataset blocks. Supports all primitive types.
 *
 * @author Stephan Saalfeld &lt;saalfelds@janelia.hhmi.org&gt;
 * @author Philipp Hanslovsky &lt;hanslovskyp@janelia.hhmi.org&gt;
 *
 * @param <T> the type parameter
 */
public class N5CellLoader<T extends NativeType<T>> implements CellLoader<T> {

	private final N5Reader n5;

	private final String dataset;

	private final int[] cellDimensions;

	private final DatasetAttributes attributes;

	private final BiConsumer<SingleCellArrayImg<T, ?>, DataBlock<?>> copyFromBlock;

	private final Consumer<IterableInterval<T>> blockNotFoundHandler;

	/**
	 *
	 * Calls
	 * {@link N5CellLoader#N5CellLoader(N5Reader, String, int[], Consumer)} with
	 * {@code blockNotFoundHandler} defaulting to no action ({@code img -> {}})
	 *
	 * @param n5 the n5 reader
	 * @param dataset the dataset path
	 * @param cellDimensions size of the cell (block)
	 * @throws IOException the execption
	 */
	public N5CellLoader(final N5Reader n5, final String dataset, final int[] cellDimensions) throws IOException {

		this(n5, dataset, cellDimensions, img -> {});
	}

	/**
	 *
	 * @param n5 the n5 reader
	 * @param dataset the dataset path
	 * @param cellDimensions size of the cell (block)
	 * @param blockNotFoundHandler
	 *            Sets block contents if the appropriate {@link N5Reader}
	 *            returns {@code null} for that block.
	 * @throws IOException the exception
	 */
	public N5CellLoader(
			final N5Reader n5,
			final String dataset,
			final int[] cellDimensions,
			final Consumer<IterableInterval<T>> blockNotFoundHandler)
			throws IOException {

		super();
		this.n5 = n5;
		this.dataset = dataset;
		this.cellDimensions = cellDimensions;
		this.attributes = n5.getDatasetAttributes(dataset);
		this.copyFromBlock = createCopy(attributes.getDataType());
		this.blockNotFoundHandler = blockNotFoundHandler;
		if (!Arrays.equals(this.cellDimensions, attributes.getBlockSize()))
			throw new RuntimeException(
					"Cell dimensions inconsistent! " + " " + Arrays.toString(cellDimensions) + " "
							+ Arrays.toString(attributes.getBlockSize()));
	}

	@Override
	public void load(final SingleCellArrayImg<T, ?> cell) {

		final long[] gridPosition = new long[cell.numDimensions()];
		for (int d = 0; d < gridPosition.length; ++d)
			gridPosition[d] = cell.min(d) / cellDimensions[d];
		final DataBlock<?> block;
		block = n5.readBlock(dataset, attributes, gridPosition);

		if (block == null)
			blockNotFoundHandler.accept(cell);
		else
			copyFromBlock.accept(cell, block);

	}

	public static <T extends Type<T>> void burnIn(
			final RandomAccessibleInterval<T> source,
			final RandomAccessibleInterval<T> target) {

		for (Cursor<T> s = Views.flatIterable(source).cursor(), t = Views.flatIterable(target).cursor(); t.hasNext();)
			t.next().set(s.next());
	}

	/**
	 * Copies data from source into target and tests whether all values equal a
	 * reference value.
	 *
	 * @param <T> the type parameter
	 * @param source source image
	 * @param target target image
	 * @param reference reference value
	 * @return true if all values were equal to the reference
	 */
	public static <T extends Type<T>> boolean burnInTestAllEqual(
			final RandomAccessibleInterval<T> source,
			final RandomAccessibleInterval<T> target,
			final T reference) {

		boolean equal = true;
		for (Cursor<T> s = Views.flatIterable(source).cursor(), t = Views.flatIterable(target).cursor(); t.hasNext();) {
			final T ts = s.next();
			equal &= reference.valueEquals(ts);
			t.next().set(ts);
		}

		return equal;
	}

	public static <T extends NativeType<T>, I extends RandomAccessibleInterval<T> & IterableInterval<T>> BiConsumer<I, DataBlock<?>> createCopy(
			final DataType dataType) {

		switch (dataType) {
		case INT8:
		case UINT8:
			return (a, b) -> {
				if (sizeEquals(a, b)) {
					final byte[] data = (byte[])b.getData();
					@SuppressWarnings("unchecked")
					final Cursor<? extends GenericByteType<?>> c = (Cursor<? extends GenericByteType<?>>)a.cursor();
					for (int i = 0; i < data.length; ++i)
						c.next().setByte(data[i]);
				} else
					copyIntersection(a, b, dataType);
			};
		case INT16:
		case UINT16:
			return (a, b) -> {
				if (sizeEquals(a, b)) {
					final short[] data = (short[])b.getData();
					@SuppressWarnings("unchecked")
					final Cursor<? extends GenericShortType<?>> c = (Cursor<? extends GenericShortType<?>>)a.cursor();
					for (int i = 0; i < data.length; ++i)
						c.next().setShort(data[i]);
				} else
					copyIntersection(a, b, dataType);
			};
		case INT32:
		case UINT32:
			return (a, b) -> {
				if (sizeEquals(a, b)) {
					final int[] data = (int[])b.getData();
					@SuppressWarnings("unchecked")
					final Cursor<? extends GenericIntType<?>> c = (Cursor<? extends GenericIntType<?>>)a.cursor();
					for (int i = 0; i < data.length; ++i)
						c.next().setInt(data[i]);
				} else
					copyIntersection(a, b, dataType);
			};
		case INT64:
		case UINT64:
			return (a, b) -> {
				if (sizeEquals(a, b)) {
					final long[] data = (long[])b.getData();
					@SuppressWarnings("unchecked")
					final Cursor<? extends GenericLongType<?>> c = (Cursor<? extends GenericLongType<?>>)a.cursor();
					for (int i = 0; i < data.length; ++i)
						c.next().setLong(data[i]);
				} else
					copyIntersection(a, b, dataType);
			};
		case FLOAT32:
			return (a, b) -> {
				if (sizeEquals(a, b)) {
					final float[] data = (float[])b.getData();
					@SuppressWarnings("unchecked")
					final Cursor<? extends FloatType> c = (Cursor<? extends FloatType>)a.cursor();
					for (int i = 0; i < data.length; ++i)
						c.next().set(data[i]);
				} else
					copyIntersection(a, b, dataType);
			};
		case FLOAT64:
			return (a, b) -> {
				if (sizeEquals(a, b)) {
					final double[] data = (double[])b.getData();
					@SuppressWarnings("unchecked")
					final Cursor<? extends DoubleType> c = (Cursor<? extends DoubleType>)a.cursor();
					for (int i = 0; i < data.length; ++i)
						c.next().set(data[i]);
				} else
					copyIntersection(a, b, dataType);
			};
		default:
			throw new IllegalArgumentException("Type " + dataType.name() + " not supported!");
		}
	}

	/**
	 *
	 * @param <T> type parameter 
	 * @param <I> interval type
	 * @param defaultValue the default value
	 * @return {@link Consumer} that sets all values of its argument to
	 *         {@code defaultValue}.
	 */
	public static <T extends Type<T>, I extends IterableInterval<T>> Consumer<I> setToDefaultValue(
			final T defaultValue) {

		return rai -> rai.forEach(pixel -> pixel.set(defaultValue));
	}

	private static boolean sizeEquals(final Interval a, final DataBlock<?> b) {

		final int[] dataBlockSize = b.getSize();
		for (int d = 0; d < dataBlockSize.length; ++d) {
			if (a.dimension(d) != dataBlockSize[d])
				return false;
		}
		return true;
	}

	@SuppressWarnings("rawtypes")
	private static ArrayImg dataBlock2ArrayImg(
			final DataBlock<?> dataBlock,
			final DataType dataType) {

		final int[] dataBlockSize = dataBlock.getSize();
		final long[] dims = new long[dataBlockSize.length];
		for (int d = 0; d < dataBlockSize.length; ++d)
			dims[d] = dataBlockSize[d];

		switch (dataType) {
		case INT8:
			return ArrayImgs.bytes((byte[])dataBlock.getData(), dims);
		case UINT8:
			return ArrayImgs.unsignedBytes((byte[])dataBlock.getData(), dims);
		case INT16:
			return ArrayImgs.shorts((short[])dataBlock.getData(), dims);
		case UINT16:
			return ArrayImgs.unsignedShorts((short[])dataBlock.getData(), dims);
		case INT32:
			return ArrayImgs.ints((int[])dataBlock.getData(), dims);
		case UINT32:
			return ArrayImgs.unsignedInts((int[])dataBlock.getData(), dims);
		case INT64:
			return ArrayImgs.longs((long[])dataBlock.getData(), dims);
		case UINT64:
			return ArrayImgs.unsignedLongs((long[])dataBlock.getData(), dims);
		case FLOAT32:
			return ArrayImgs.floats((float[])dataBlock.getData(), dims);
		case FLOAT64:
			return ArrayImgs.doubles((double[])dataBlock.getData(), dims);
		default:
			return null;
		}
	}

	private static <T extends NativeType<T>, I extends RandomAccessibleInterval<T> & IterableInterval<T>> void copyIntersection(
			final I a,
			final DataBlock<?> b,
			final DataType dataType) {

		@SuppressWarnings("unchecked")
		final ArrayImg<T, ?> block = dataBlock2ArrayImg(b, dataType);
		final IntervalView<T> za = Views.zeroMin(a);
		final FinalInterval intersection = Intervals.intersect(block, za);
		final Cursor<T> c = Views.interval(za, intersection).cursor();
		final Cursor<T> d = Views.interval(block, intersection).cursor();
		while (c.hasNext())
			c.next().set(d.next());
	}
}

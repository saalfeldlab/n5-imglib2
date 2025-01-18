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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.GzipCompression;
import org.janelia.saalfeldlab.n5.KeyValueAccess;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.RawCompression;
import org.janelia.saalfeldlab.n5.ShardedDatasetAttributes;
import org.janelia.saalfeldlab.n5.ShortArrayDataBlock;
import org.janelia.saalfeldlab.n5.codec.BytesCodec;
import org.janelia.saalfeldlab.n5.codec.Codec;
import org.janelia.saalfeldlab.n5.codec.DeterministicSizeCodec;
import org.janelia.saalfeldlab.n5.codec.checksum.Crc32cChecksumCodec;
import org.janelia.saalfeldlab.n5.shard.Shard;
import org.janelia.saalfeldlab.n5.shard.ShardIndex;
import org.janelia.saalfeldlab.n5.shard.ShardParameters;
import org.janelia.saalfeldlab.n5.shard.ShardingCodec.IndexLocation;
import org.janelia.saalfeldlab.n5.util.GridIterator;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import net.imglib2.Cursor;
import net.imglib2.FinalInterval;
import net.imglib2.Interval;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.cache.img.CachedCellImg;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.array.ArrayRandomAccess;
import net.imglib2.img.basictypeaccess.ShortAccess;
import net.imglib2.img.basictypeaccess.array.IntArray;
import net.imglib2.img.basictypeaccess.array.ShortArray;
import net.imglib2.img.basictypeaccess.volatiles.VolatileAccess;
import net.imglib2.img.cell.CellGrid;
import net.imglib2.img.cell.CellGrid.CellIntervals;
import net.imglib2.loops.LoopBuilder;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.IntegerType;
import net.imglib2.type.numeric.NumericType;
import net.imglib2.type.numeric.integer.IntType;
import net.imglib2.type.numeric.integer.UnsignedShortType;
import net.imglib2.util.Intervals;
import net.imglib2.util.Pair;
import net.imglib2.view.IntervalView;
import net.imglib2.view.Views;

public class N5UtilsTest {

	static private String testDirPath;

	static private String datasetName = "/test/group/dataset";

	static private String shardDatasetName = "/test/group/shardDataset";

	static private long[] dimensions = new long[]{20, 28, 36};

	static private int[] blockSize = new int[]{5, 7, 9};

	static private int[] shardSize = new int[]{10, 14, 18};

	static short[] data;

	static short[] excessData;

	static private N5Writer n5;

	private static final int MAX_NUM_CACHE_ENTRIES = 10;

	private static final String EMPTY_DATASET = "/test/group/empty-dataset";

	private static final int EMPTY_BLOCK_VALUE = 123;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {

		final File testDir;
		try {
			testDir = Files.createTempDirectory("n5utils-test-").toFile();
			testDir.deleteOnExit();
			testDirPath = testDir.getAbsolutePath();
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}

		if (!(testDir.exists() && testDir.isDirectory()))
			throw new IOException("Could not create test directory for HDF5Utils test.");

		n5 = new N5FSWriter(testDirPath);

		final Random rnd = new Random();

		data = new short[(int)(dimensions[0] * dimensions[1] * dimensions[2])];
		for (int i = 0; i < data.length; ++i)
			data[i] = (short)rnd.nextInt();

		excessData = new short[(int)((dimensions[0] + 2) * (dimensions[1] + 3) * (dimensions[2] + 4))];
		for (int i = 0; i < excessData.length; ++i)
			excessData[i] = (short)rnd.nextInt();

		n5.createDataset(
				EMPTY_DATASET,
				dimensions,
				blockSize,
				N5Utils.dataType(new UnsignedShortType()),
				new GzipCompression());
	}

	@AfterClass
	public static void rampDownAfterClass() throws Exception {

		n5.remove("");
	}

	@Before
	public void setUp() throws Exception {}

	@After
	public void tearDown() throws Exception {}

	@Test
	public void testSaveAndOpen() throws InterruptedException, ExecutionException {

		testSaveAndOpenHelper(
			datasetName,
			img -> N5Utils.save(img, n5, datasetName, blockSize, new RawCompression()),
			img -> {
				try {
					N5Utils.saveRegion(img, n5, datasetName);
				} catch (Exception e) {
					fail();
				}
			},
			img -> {
				final ExecutorService exec = Executors.newFixedThreadPool(4);
				try {
					N5Utils.save(img, n5, datasetName, blockSize, new RawCompression(), exec);
					exec.shutdown();
				} catch (Exception e) {
					fail();
				}
			},
			img -> {
				final ExecutorService exec = Executors.newFixedThreadPool(4);
				try {
					N5Utils.saveRegion(img, n5, datasetName, exec);
					exec.shutdown();
				} catch (Exception e) {
					fail();
				}
			}
		);
	}

	@Test
	public void testSaveAndOpenShard() throws InterruptedException, ExecutionException {

		testSaveAndOpenHelper(
			shardDatasetName,
			img -> N5Utils.save(img, n5, shardDatasetName, shardSize, blockSize, new RawCompression()),
			img -> {
				try {
					N5Utils.saveRegion(img, n5, shardDatasetName);
				} catch (Exception e) {
					fail();
				}
			},
			img -> {
				final ExecutorService exec = Executors.newFixedThreadPool(4);
				try {
					N5Utils.save(img, n5, shardDatasetName, shardSize, blockSize, new RawCompression(), exec);
					exec.shutdown();
				} catch (Exception e) {
					fail();
				}
			},
			img -> {
				final ExecutorService exec = Executors.newFixedThreadPool(4);
				try {
					N5Utils.saveRegion(img, n5, shardDatasetName, exec);
					exec.shutdown();
				} catch (Exception e) {
					fail();
				}
			}
		);
	}

	public <T extends IntegerType<T> & NativeType<T>> void testSaveAndOpenHelper(
			final String dataset,
			final Consumer<RandomAccessibleInterval<T>> save,
			final Consumer<RandomAccessibleInterval<T>> saveRegion,
			final Consumer<RandomAccessibleInterval<T>> saveParallel,
			final Consumer<RandomAccessibleInterval<T>> saveRegionParallel) {

		final ArrayImg<T, ?> img = (ArrayImg<T, ?>) ArrayImgs.unsignedShorts(data, dimensions);
		save.accept(img);
		RandomAccessibleInterval<T> loaded = N5Utils.open(n5, dataset);
		for (final Pair<T, T> pair : Views
				.flatIterable(Views.interval(Views.pair(img, loaded), img)))
			Assert.assertEquals(pair.getA().getInteger(), pair.getB().getInteger());

		// test save region single thread
		saveRegion.accept(Views.translate(img, dimensions));
		loaded = N5Utils.open(n5, dataset);
		final long[] expectedPaddedDims = Arrays.stream(dimensions).map(x -> 2 * x).toArray();
		final long[] newDims = Intervals.dimensionsAsLongArray(loaded);
		Assert.assertArrayEquals("saveRegion padded dims", expectedPaddedDims, newDims);

		final IntervalView<T> loadedSubset = Views.offsetInterval(loaded, dimensions, dimensions);
		for (final Pair<T, T> pair : Views
				.flatIterable(Views.interval(Views.pair(img, loadedSubset), img)))
			Assert.assertEquals(pair.getA().getInteger(), pair.getB().getInteger());

		// test multithreaded writing
		saveParallel.accept(img);;

		loaded = N5Utils.open(n5, dataset);
		for (final Pair<T, T> pair : Views
				.flatIterable(Views.interval(Views.pair(img, loaded), img)))
			Assert.assertEquals(pair.getA().getInteger(), pair.getB().getInteger());

		// test save region multi thread
		saveRegionParallel.accept(Views.translate(img, dimensions));

		loaded = N5Utils.open(n5, dataset);
		final IntervalView<T> loadedSubsetParallel = Views.offsetInterval(loaded, dimensions, dimensions);
		for (final Pair<T, T> pair : Views.flatIterable(Views.interval(Views.pair(img, loadedSubsetParallel), img)))
			Assert.assertEquals(pair.getA().getInteger(), pair.getB().getInteger());
	}

	@Test
	public void testSaveNonEmpty() throws InterruptedException, ExecutionException {

		final String datasetPath = "nonEmptyTest";
		final ArrayImg<UnsignedShortType, ?> img = ArrayImgs.unsignedShorts(dimensions);

		// dimensions are : {20, 28, 36}
		// block size is 	{ 5,  7,  9}
		// 4x4x4 block grid, set only "diagonal blocks" (i,i,i) i in [0,3]
		ArrayRandomAccess<UnsignedShortType> ra = img.randomAccess();
		ra.setPositionAndGet(0,0,0).set(1);
		ra.setPositionAndGet(5,7,9).set(1);
		ra.setPositionAndGet(10,14,18).set(1);
		ra.setPositionAndGet(15,21,27).set(1);

		final UnsignedShortType zero = new UnsignedShortType();
		zero.setZero();

		final DatasetAttributes attrs = new DatasetAttributes(
				img.dimensionsAsLongArray(),
				blockSize, 
				DataType.UINT16,
				new RawCompression());

		final N5FSWriter n5fs = (N5FSWriter)n5;
		final KeyValueAccess kva = n5fs.getKeyValueAccess();

		n5.createDataset(datasetPath, attrs);
		N5Utils.saveNonEmptyBlock(img, n5, datasetPath, zero);

		final long[] p = new long[3];
		final CellIntervals blocks = new CellGrid(dimensions, blockSize).cellIntervals();
		final Cursor<Interval> c = blocks.cursor();
		while( c.hasNext()) {
			c.fwd();
			c.localize(p);
			final String blockPath = n5fs.absoluteDataBlockPath(datasetPath, p);
			if( p[0] == p[1] && p[0] == p[2] )
				assertTrue(kva.exists(blockPath));
			else
				assertFalse(kva.exists(blockPath));
		}
	}

	@Test
	public void testSaveNonEmptyShard() throws InterruptedException, ExecutionException {

		final String datasetPath = "nonEmptyTestShard";
		final ArrayImg<UnsignedShortType, ?> img = ArrayImgs.unsignedShorts(dimensions);

		// dimensions are : {20, 28, 36}
		// block size is 	{ 5,  7,  9}
		// shard size is 	{10, 14, 18}
		// 4x4x4 block grid, set only "diagonal blocks" (i,i,i) i in [0,3]
		// 2x2x2 shard grid, set only "diagonal blocks" (i,i,i) i in [0,3]
		ArrayRandomAccess<UnsignedShortType> ra = img.randomAccess();
		ra.setPositionAndGet(0,0,0).set(1);
		ra.setPositionAndGet(5,7,9).set(1);
		ra.setPositionAndGet(10,14,18).set(1);
		ra.setPositionAndGet(15,21,27).set(1);

		final UnsignedShortType zero = new UnsignedShortType();
		zero.setZero();

		ShardedDatasetAttributes attrs = new ShardedDatasetAttributes(
				dimensions,
				shardSize,
				blockSize,
				DataType.UINT16,
				new Codec[]{new BytesCodec(), new GzipCompression(4)},
				new DeterministicSizeCodec[]{new BytesCodec(), new Crc32cChecksumCodec()},
				IndexLocation.END
		);

		final N5FSWriter n5fs = (N5FSWriter)n5;
		final KeyValueAccess kva = n5fs.getKeyValueAccess();

		n5.remove(datasetPath);
		n5.createDataset(datasetPath, attrs);
		N5Utils.saveNonEmptyBlock(img, n5, datasetPath, zero);

		// ensure that only shards [0,0,0] and [1,1,1] were written
		final long[] p = new long[3];
		final CellIntervals shards = new CellGrid(dimensions, shardSize).cellIntervals();
		final Cursor<Interval> c = shards.cursor();
		while( c.hasNext()) {
			c.fwd();
			c.localize(p);
			final String blockPath = n5fs.absoluteDataBlockPath(datasetPath, p);
			if( p[0] == p[1] && p[0] == p[2] )
				assertTrue(kva.exists(blockPath));
			else
				assertFalse(kva.exists(blockPath));
		}

		// read each shard
		final ShardIndex index0 = n5.readShard(datasetPath, attrs, 0, 0, 0).getIndex();
		final ShardIndex index1 = n5.readShard(datasetPath, attrs, 1, 1, 1).getIndex();

		final int[] indexPos = new int[3];
		final CellIntervals blocks = new CellGrid(GridIterator.int2long(shardSize), blockSize).cellIntervals();
		final Cursor<Interval> bcursor = blocks.cursor();
		while (bcursor.hasNext()) {
			bcursor.fwd();
			bcursor.localize(indexPos);

			if( indexPos[0] == indexPos[1] && indexPos[0] == indexPos[2] ) {
				assertTrue(index0.exists(indexPos));
				assertTrue(index1.exists(indexPos));
			}
			else {
				assertFalse(index0.exists(indexPos));
				assertFalse(index1.exists(indexPos));
			}
		}
	}

	@Test
	public void testOpenWithBoundedSoftRefCache() throws IOException {

		// existing dataset
		{
			final ArrayImg<UnsignedShortType, ?> img = ArrayImgs.unsignedShorts(data, dimensions);
			N5Utils.save(img, n5, datasetName, blockSize, new RawCompression());
			final RandomAccessibleInterval<UnsignedShortType> loaded = N5Utils
					.openWithBoundedSoftRefCache(n5, datasetName, MAX_NUM_CACHE_ENTRIES);
			for (final Pair<UnsignedShortType, UnsignedShortType> pair : Views
					.flatIterable(Views.interval(Views.pair(img, loaded), img)))
				Assert.assertEquals(pair.getA().get(), pair.getB().get());
			MatcherAssert
					.assertThat(
							((CachedCellImg<UnsignedShortType, ?>)loaded).getAccessType(),
							CoreMatchers.instanceOf(ShortAccess.class));
		}

		// empty dataset with default value
		{
			final RandomAccessibleInterval<UnsignedShortType> loaded = N5Utils
					.openWithBoundedSoftRefCache(
							n5,
							EMPTY_DATASET,
							MAX_NUM_CACHE_ENTRIES,
							new UnsignedShortType(EMPTY_BLOCK_VALUE));
			Views.iterable(loaded).forEach(val -> Assert.assertEquals(EMPTY_BLOCK_VALUE, val.get()));
			MatcherAssert
					.assertThat(
							((CachedCellImg<UnsignedShortType, ?>)loaded).getAccessType(),
							CoreMatchers.instanceOf(ShortAccess.class));
		}
	}

	@Test
	public void testVolatileOpenWithBoundedSoftRefCache() throws IOException {

		// existing dataset
		{
			final ArrayImg<UnsignedShortType, ?> img = ArrayImgs.unsignedShorts(data, dimensions);
			N5Utils.save(img, n5, datasetName, blockSize, new RawCompression());
			final RandomAccessibleInterval<UnsignedShortType> loaded = N5Utils
					.openVolatileWithBoundedSoftRefCache(n5, datasetName, MAX_NUM_CACHE_ENTRIES);
			for (final Pair<UnsignedShortType, UnsignedShortType> pair : Views
					.flatIterable(Views.interval(Views.pair(img, loaded), img)))
				Assert.assertEquals(pair.getA().get(), pair.getB().get());
			Assert.assertEquals(UnsignedShortType.class, loaded.getType().getClass());
			MatcherAssert
					.assertThat(
							((CachedCellImg<UnsignedShortType, ?>)loaded).getAccessType(),
							CoreMatchers.instanceOf(VolatileAccess.class));
			MatcherAssert
					.assertThat(
							((CachedCellImg<UnsignedShortType, ?>)loaded).getAccessType(),
							CoreMatchers.instanceOf(ShortAccess.class));
		}

		// empty dataset with default value
		{
			final RandomAccessibleInterval<UnsignedShortType> loaded = N5Utils
					.openVolatileWithBoundedSoftRefCache(
							n5,
							EMPTY_DATASET,
							MAX_NUM_CACHE_ENTRIES,
							new UnsignedShortType(EMPTY_BLOCK_VALUE));
			Views.iterable(loaded).forEach(val -> Assert.assertEquals(EMPTY_BLOCK_VALUE, val.get()));
			Assert.assertEquals(UnsignedShortType.class, loaded.getType().getClass());
			MatcherAssert
					.assertThat(
							((CachedCellImg<UnsignedShortType, ?>)loaded).getAccessType(),
							CoreMatchers.instanceOf(VolatileAccess.class));
			MatcherAssert
					.assertThat(
							((CachedCellImg<UnsignedShortType, ?>)loaded).getAccessType(),
							CoreMatchers.instanceOf(ShortAccess.class));
		}
	}

	@Test
	public void testDelete() {

		final ArrayImg<UnsignedShortType, ?> img = ArrayImgs.unsignedShorts(data, dimensions);
		N5Utils.save(img, n5, datasetName, blockSize, new RawCompression());
		RandomAccessibleInterval<UnsignedShortType> loaded = N5Utils.open(n5, datasetName);
		for (final Pair<UnsignedShortType, UnsignedShortType> pair : Views
				.flatIterable(Views.interval(Views.pair(img, loaded), img)))
			Assert.assertEquals(pair.getA().get(), pair.getB().get());

		N5Utils.deleteBlock(new FinalInterval(dimensions), n5, datasetName);

		loaded = N5Utils.open(n5, datasetName);
		for (final UnsignedShortType val : Views.iterable(loaded))
			Assert.assertEquals(0, val.get());
	}

	private short[] fillData(final int[] size) {

		return Arrays.copyOf(excessData, Arrays.stream(size).reduce(1, (a, b) -> a * b));
	}

	@Test
	public void testBlockSize() throws IOException {

		n5.remove(datasetName);
		final DatasetAttributes datasetAttributes = new DatasetAttributes(
				dimensions,
				blockSize,
				DataType.UINT16,
				new GzipCompression());
		n5.createDataset(datasetName, datasetAttributes);

		final int[] blockSize000 = new int[blockSize.length];
		Arrays.setAll(blockSize000, i -> blockSize[i] - 2);
		final ShortArrayDataBlock block000 = new ShortArrayDataBlock(
				blockSize000,
				new long[]{0, 0, 0},
				fillData(blockSize000));
		n5.writeBlock(datasetName, datasetAttributes, block000);

		final int[] blockSize001 = new int[blockSize.length];
		Arrays.setAll(blockSize001, i -> blockSize[i] + 2);
		final ShortArrayDataBlock block001 = new ShortArrayDataBlock(
				blockSize001,
				new long[]{0, 0, 1},
				fillData(blockSize001));
		n5.writeBlock(datasetName, datasetAttributes, block001);

		final RandomAccessibleInterval<UnsignedShortType> img = N5Utils.open(n5, datasetName);

		final IntervalView<UnsignedShortType> interval000 = Views
				.interval(
						img,
						new long[]{0, 0, 0},
						new long[]{blockSize000[0] - 1, blockSize000[1] - 1, blockSize000[2] - 1});

		int i = 0;
		for (final UnsignedShortType t : interval000)
			assertTrue(t.getShort() == excessData[i++]);

		final IntervalView<UnsignedShortType> interval001 = Views
				.interval(
						img,
						new long[]{0, 0, blockSize[2]},
						new long[]{blockSize[0] - 1, blockSize[1] - 1, blockSize[2] + blockSize[2] - 1});

		i = 0;
		final ArrayImg<UnsignedShortType, ShortArray> referenceDataImg = ArrayImgs
				.unsignedShorts(
						excessData,
						blockSize001[0],
						blockSize001[1],
						blockSize001[2]);
		final Cursor<UnsignedShortType> c = Views
				.interval(
						referenceDataImg,
						new long[]{0, 0, 0},
						new long[]{blockSize[0] - 1, blockSize[1] - 1, blockSize[2] - 1})
				.cursor();
		final Cursor<UnsignedShortType> d = interval001.cursor();
		while (c.hasNext())
			assertTrue(c.next().valueEquals(d.next()));
	}

	@Test
	public void testShard() throws IOException {

		final String shardDset = "shardDataset";

		final int nx = 20;
		final int ny = 16;
		final int[] data = IntStream.range(0, nx * ny).toArray();
		final ArrayImg<IntType, IntArray> img = ArrayImgs.ints(data, nx, ny);

		final int[] readData = new int[data.length];
		final ArrayImg<IntType, IntArray> imgRead = ArrayImgs.ints(readData, nx, ny);

		final long[] imgSize = new long[] { nx, ny };
		final int[] shardSize = new int[] { 10, 8 };
		final int[] blkSize = new int[] { 5, 4 };

		n5.remove(shardDset);
		N5Utils.save(img, n5, shardDset, shardSize, blkSize, new RawCompression());

		assertTrue(n5.datasetExists(shardDset));

		final DatasetAttributes attrs = n5.getDatasetAttributes(shardDset);
		assertTrue("attributes not sharded", attrs instanceof ShardParameters);
		assertArrayEquals("block size incorrect", blkSize, attrs.getBlockSize());
		assertArrayEquals("shard size incorrect", shardSize, ((ShardParameters) attrs).getShardSize());

		CachedCellImg<IntType, ?> tmp = N5Utils.open(n5, shardDset);
		LoopBuilder.setImages(tmp, imgRead).forEachPixel((x, y) -> {
			y.set(x);
		});

		assertArrayEquals("data incorrect", data, readData);
	}

}
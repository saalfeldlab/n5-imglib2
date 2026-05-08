package org.janelia.saalfeldlab.n5.imglib2;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
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
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.GzipCompression;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.RawCompression;
import org.janelia.saalfeldlab.n5.ShortArrayDataBlock;
import org.janelia.saalfeldlab.n5.codec.DataCodecInfo;
import org.janelia.saalfeldlab.n5.codec.N5BlockCodecInfo;
import org.janelia.saalfeldlab.n5.codec.RawBlockCodecInfo;
import org.janelia.saalfeldlab.n5.shard.DefaultShardCodecInfo;
import org.janelia.saalfeldlab.n5.shard.ShardIndex.IndexLocation;
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
import net.imglib2.img.basictypeaccess.array.ShortArray;
import net.imglib2.img.basictypeaccess.volatiles.VolatileAccess;
import net.imglib2.img.cell.CellGrid;
import net.imglib2.img.cell.CellGrid.CellIntervals;
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
			n5,
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

	public void testSaveAndOpenHelper(
			final N5Reader n5r,
			final String dataset,
			final Consumer<RandomAccessibleInterval<UnsignedShortType>> save,
			final Consumer<RandomAccessibleInterval<UnsignedShortType>> saveRegion,
			final Consumer<RandomAccessibleInterval<UnsignedShortType>> saveParallel,
			final Consumer<RandomAccessibleInterval<UnsignedShortType>> saveRegionParallel) {

		ArrayImg<UnsignedShortType, ShortArray> img = ArrayImgs.unsignedShorts(data, dimensions);
		save.accept(img);
		RandomAccessibleInterval<UnsignedShortType> loaded = N5Utils.open(n5r, dataset);
		for (final Pair<UnsignedShortType, UnsignedShortType> pair : Views
				.flatIterable(Views.interval(Views.pair(img, loaded), img)))
			Assert.assertEquals(pair.getA().getInteger(), pair.getB().getInteger());

		// test save region single thread
		saveRegion.accept(Views.translate(img, dimensions));
		loaded = N5Utils.open(n5r, dataset);
		final long[] expectedPaddedDims = Arrays.stream(dimensions).map(x -> 2 * x).toArray();
		final long[] newDims = Intervals.dimensionsAsLongArray(loaded);
		Assert.assertArrayEquals("saveRegion padded dims", expectedPaddedDims, newDims);

		final IntervalView<UnsignedShortType> loadedSubset = Views.offsetInterval(loaded, dimensions, dimensions);
		for (final Pair<UnsignedShortType, UnsignedShortType> pair : Views
				.flatIterable(Views.interval(Views.pair(img, loadedSubset), img)))
			Assert.assertEquals(pair.getA().getInteger(), pair.getB().getInteger());

		// test multithreaded writing
		saveParallel.accept(img);;

		loaded = N5Utils.open(n5r, dataset);
		for (final Pair<UnsignedShortType, UnsignedShortType> pair : Views
				.flatIterable(Views.interval(Views.pair(img, loaded), img)))
			Assert.assertEquals(pair.getA().getInteger(), pair.getB().getInteger());

		// test save region multi thread
		saveRegionParallel.accept(Views.translate(img, dimensions));

		loaded = N5Utils.open(n5r, dataset);
		final IntervalView<UnsignedShortType> loadedSubsetParallel = Views.offsetInterval(loaded, dimensions, dimensions);
		for (final Pair<UnsignedShortType, UnsignedShortType> pair : Views.flatIterable(Views.interval(Views.pair(img, loadedSubsetParallel), img)))
			Assert.assertEquals(pair.getA().getInteger(), pair.getB().getInteger());
	}

	@Test
	public void testSaveNonEmpty() throws InterruptedException, ExecutionException {

		final String datasetPath = "nonEmptyTest";
		final UnsignedShortType zero = new UnsignedShortType();
		zero.setZero();

		testSaveNonEmptyShardHelper(
				datasetPath,
				this::datasetAttributes,
				img -> { N5Utils.saveNonEmptyBlock(img, n5, datasetPath, zero); });
	}

	@Test
	public void testSaveNonEmptyShard() throws InterruptedException, ExecutionException {

		final String datasetPath = "nonEmptyTestShard";
		final UnsignedShortType zero = new UnsignedShortType();
		zero.setZero();

		final DatasetAttributes shardedAttributes = shardedDatasetAttributes();

		// need to pass the DatasetAttributes to saveNonEmptyBlock because
		// the n5 format does not support sharding, but wqe can get around it
		// by using the ShardedN5Writer instance and passing a DatasetAttributes instance with the correct codecs
		testSaveNonEmptyShardHelper(
				datasetPath,
				() -> shardedAttributes,
				img -> { N5Utils.saveNonEmptyBlock(img, n5, datasetPath, shardedAttributes, zero); });

		final String datasetPath2 = "nonEmptyTestShard2";
		testSaveNonEmptyShardHelper(
				datasetPath2,
				() -> shardedAttributes,
				img -> { N5Utils.saveNonEmptyBlock(img, n5, datasetPath2, shardedAttributes, zero); });
	}

	private DatasetAttributes shardedDatasetAttributes() {

		final DefaultShardCodecInfo blockCodec = new DefaultShardCodecInfo(
				blockSize,
				new N5BlockCodecInfo(),
				new DataCodecInfo[]{new RawCompression()},
				new RawBlockCodecInfo(),
				new DataCodecInfo[]{new RawCompression()},
				IndexLocation.END);

		return new DatasetAttributes(
				dimensions,
				shardSize,
				DataType.UINT16,
				blockCodec);
	}

	private DatasetAttributes datasetAttributes() {
		return new DatasetAttributes(dimensions, blockSize, DataType.UINT16,
				new RawCompression());
	}

	public void testSaveNonEmptyShardHelper(
			final String datasetPath,
			final Supplier<DatasetAttributes> datasetAttributes,
			final Consumer<RandomAccessibleInterval<UnsignedShortType>> saveNonEmpty
			) throws InterruptedException, ExecutionException {

		final ArrayImg<UnsignedShortType, ?> img = ArrayImgs.unsignedShorts(dimensions);

		// dimensions are : {20, 28, 36}
		// block size is 	{ 5,  7,  9}
		// shard size is 	{10, 14, 18}
		// 4x4x4 block grid, set only "diagonal blocks" (i,i,i) i in [0,3]
		// as a result, in the  2x2x2 shard grid, only the "diagonal shards" (i,i,i) i in [0,1]
		// will exist (because they contain non-empty blocks)
		ArrayRandomAccess<UnsignedShortType> ra = img.randomAccess();
		ra.setPositionAndGet(0,0,0).set(1);
		ra.setPositionAndGet(5,7,9).set(1);
		ra.setPositionAndGet(10,14,18).set(1);
		ra.setPositionAndGet(15,21,27).set(1);

		final UnsignedShortType zero = new UnsignedShortType();
		zero.setZero();

		final DatasetAttributes attrs = datasetAttributes.get();

		n5.remove(datasetPath);
		DatasetAttributes attributes = n5.createDataset(datasetPath, attrs);
		saveNonEmpty.accept(img);

		final CellIntervals blocks = new CellGrid(dimensions, blockSize).cellIntervals();
		final Cursor<Interval> c = blocks.cursor();
		final long[] blockPos = new long[3];
		while (c.hasNext()) {
			c.fwd();
			c.localize(blockPos);
			final DataBlock<?> blk = n5.readChunk(datasetPath, attributes, blockPos);
			if (blockPos[0] == blockPos[1] && blockPos[0] == blockPos[2]) {
				assertNotNull(blk);
			} else {
				assertNull(blk);
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
	public void testDeleteAllBlocks() {

		final ArrayImg<UnsignedShortType, ?> img = ArrayImgs.unsignedShorts(data, dimensions);
		N5Utils.save(img, n5, datasetName, blockSize, new RawCompression());

		CachedCellImg<UnsignedShortType, ?> loaded = N5Utils.open(n5, datasetName);
		assertOnlyBlocksDeleted( img, loaded, p -> false);

		N5Utils.deleteBlock(new FinalInterval(dimensions), n5, datasetName);
		assertOnlyBlocksDeleted( img, N5Utils.open(n5, datasetName), p -> true);
	}

	@Test
	public void testDeleteSingleBlockExplicitOffset() {

		// Overload: deleteBlock(Interval, N5Writer, String, long[] gridOffset)
		// Block grid is 4x4x4. Delete interior block (1,2,1),
		// voxels [5,9]x[14,20]x[9,17].
		final ArrayImg<UnsignedShortType, ShortArray> img = ArrayImgs.unsignedShorts(data, dimensions);
		N5Utils.save(img, n5, datasetName, blockSize, new RawCompression());

		final long[] blockPosition = {1, 2, 1};
		final Interval singleBlock = new FinalInterval(
				new long[]{blockSize[0], blockSize[1], blockSize[2]});
		N5Utils.deleteBlock(singleBlock, n5, datasetName, blockPosition);

		assertOnlyBlocksDeleted(img, N5Utils.open(n5, datasetName),
				pos -> Arrays.equals(pos, blockPosition));
	}

	@Test
	public void testDeleteSingleBlockByIntervalMin() {

		// Overload: deleteBlock(Interval, N5Writer, String)
		// Non-zero-min interval: min is the voxel origin of block (2,1,2).
		// gridOffset is derived as min[d] / blockSize[d] → {2,1,2}.
		final ArrayImg<UnsignedShortType, ShortArray> img = ArrayImgs.unsignedShorts(data, dimensions);
		N5Utils.save(img, n5, datasetName, blockSize, new RawCompression());

		final long[] target = {2, 1, 2};
		final long[] voxelMin = {
				target[0] * blockSize[0],
				target[1] * blockSize[1],
				target[2] * blockSize[2]};
		final long[] voxelMax = {
				voxelMin[0] + blockSize[0] - 1,
				voxelMin[1] + blockSize[1] - 1,
				voxelMin[2] + blockSize[2] - 1};

		N5Utils.deleteBlock(new FinalInterval(voxelMin, voxelMax), n5, datasetName);

		assertOnlyBlocksDeleted(img, N5Utils.open(n5, datasetName),
				pos -> Arrays.equals(pos, target));
	}

	@Test
	public void testDeleteBlockRegion() {

		// Overload: deleteBlock(Interval, N5Writer, String, long[] gridOffset)
		// Delete a 2x2x2 region of blocks starting at grid offset (1,1,1),
		// i.e. grid positions (1,1,1) through (2,2,2) — 8 out of 64 blocks.
		final ArrayImg<UnsignedShortType, ShortArray> img = ArrayImgs.unsignedShorts(data, dimensions);
		N5Utils.save(img, n5, datasetName, blockSize, new RawCompression());

		final long[] blockGridOffset = {1, 1, 1};
		final Interval region = new FinalInterval(new long[]{
				2 * blockSize[0],
				2 * blockSize[1],
				2 * blockSize[2]});

		N5Utils.deleteBlock(region, n5, datasetName, blockGridOffset);
		assertOnlyBlocksDeleted(img, N5Utils.open(n5, datasetName),
				pos -> (pos[0] == 1 || pos[0] == 2) &&
						(pos[1] == 1 || pos[1] == 2) &&
						(pos[2] == 1 || pos[2] == 2));
	}

	private short[] fillData(final int[] size) {

		return Arrays.copyOf(excessData, Arrays.stream(size).reduce(1, (a, b) -> a * b));
	}

	/**
	 * Iterates every block in the dataset's grid and asserts: - blocks where
	 * isDeleted returns true → all pixels are 0 - blocks where isDeleted
	 * returns false → all pixels match original
	 */
	private void assertOnlyBlocksDeleted(
			final ArrayImg<UnsignedShortType, ?> original,
			final RandomAccessibleInterval<UnsignedShortType> loaded,
			final Predicate<long[]> isDeleted) {

		final CellIntervals blocks = new CellGrid(dimensions, blockSize).cellIntervals();
		final Cursor<Interval> c = blocks.cursor();
		final long[] blockPos = new long[3];
		while (c.hasNext()) {
			final Interval block = c.next();
			c.localize(blockPos);
			final String label = "block " + Arrays.toString(blockPos);
			if (isDeleted.test(blockPos)) {
				for (final UnsignedShortType val : Views.interval(loaded, block))
					Assert.assertEquals(label + " should be 0", 0, val.get());
			} else {
				for (final Pair<UnsignedShortType, UnsignedShortType> pair : Views.flatIterable(Views.interval(Views.pair(original, loaded), block)))
					Assert.assertEquals(label + " should be intact",
							pair.getA().get(), pair.getB().get());
			}
		}
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
		n5.writeChunk(datasetName, datasetAttributes, block000);

		final int[] blockSize001 = new int[blockSize.length];
		Arrays.setAll(blockSize001, i -> blockSize[i] + 2);
		final ShortArrayDataBlock block001 = new ShortArrayDataBlock(
				blockSize001,
				new long[]{0, 0, 1},
				fillData(blockSize001));
		n5.writeChunk(datasetName, datasetAttributes, block001);

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

//	@Test
//	public void testShard() throws IOException {
//
//		final String shardDset = "shardDataset";
//
//		final int nx = 20;
//		final int ny = 16;
//		final int[] data = IntStream.range(0, nx * ny).toArray();
//		final ArrayImg<IntType, IntArray> img = ArrayImgs.ints(data, nx, ny);
//
//		final int[] readData = new int[data.length];
//		final ArrayImg<IntType, IntArray> imgRead = ArrayImgs.ints(readData, nx, ny);
//
//		final int[] shardSize = new int[] { 10, 8 };
//		final int[] blkSize = new int[] { 5, 4 };
//
//		n5sharded.remove(shardDset);
//		N5Utils.save(img, n5sharded, shardDset, shardSize, blkSize, new RawCompression());
//
//		assertTrue(n5sharded.datasetExists(shardDset));
//
//		final DatasetAttributes attrs = n5sharded.getDatasetAttributes(shardDset);
//		assertTrue("attributes not sharded", attrs.isSharded());
//		assertArrayEquals("block size incorrect", blkSize, attrs.getBlockSize());
//		assertArrayEquals("shard size incorrect", shardSize, attrs.getShardSize());
//
//		CachedCellImg<IntType, ?> tmp = N5Utils.open(n5sharded, shardDset);
//		LoopBuilder.setImages(tmp, imgRead).forEachPixel((x, y) -> {
//			y.set(x);
//		});
//
//		assertArrayEquals("data incorrect", data, readData);
//	}

}

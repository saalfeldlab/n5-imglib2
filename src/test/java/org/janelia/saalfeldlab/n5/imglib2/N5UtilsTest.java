package org.janelia.saalfeldlab.n5.imglib2;

import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.RawCompression;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.type.numeric.integer.UnsignedShortType;
import net.imglib2.util.Pair;
import net.imglib2.view.Views;

public class N5UtilsTest {

	static private String testDirPath = System.getProperty("user.home") + "/tmp/n5-imglib2-test";

	static private String datasetName = "/test/group/dataset";

	static private long[] dimensions = new long[]{11, 22, 33};

	static private int[] blockSize = new int[]{5, 7, 9};

	static short[] data;

	static private N5Writer n5;

	/**
	 * @throws java.lang.Exception
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {

		final File testDir = new File(testDirPath);
		testDir.mkdirs();
		if (!(testDir.exists() && testDir.isDirectory()))
			throw new IOException("Could not create test directory for HDF5Utils test.");

		n5 = new N5FSWriter(testDirPath);

		final Random rnd = new Random();

		data = new short[(int)(dimensions[0] * dimensions[1] * dimensions[2])];
		for (int i = 0; i < data.length; ++i)
			data[i] = (short)rnd.nextInt();
	}

	/**
	 * @throws java.lang.Exception
	 */
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

		final ArrayImg<UnsignedShortType, ?> img = ArrayImgs.unsignedShorts(data, dimensions);
		try {
			N5Utils.save(img, n5, datasetName, blockSize, new RawCompression());
			RandomAccessibleInterval<UnsignedShortType> loaded = N5Utils.open(n5, datasetName);
			for (final Pair<UnsignedShortType, UnsignedShortType> pair : Views.flatIterable(Views.interval(Views.pair(img, loaded), img)))
				Assert.assertEquals(pair.getA().get(), pair.getB().get());

			final ExecutorService exec = Executors.newFixedThreadPool(4);
			N5Utils.save(img, n5, datasetName, blockSize, new RawCompression(), exec);
			loaded = N5Utils.open(n5, datasetName);
			for (final Pair<UnsignedShortType, UnsignedShortType> pair : Views.flatIterable(Views.interval(Views.pair(img, loaded), img)))
				Assert.assertEquals(pair.getA().get(), pair.getB().get());
			exec.shutdown();

		} catch (final IOException e) {
			fail("Failed by I/O exception.");
			e.printStackTrace();
		}
	}

}

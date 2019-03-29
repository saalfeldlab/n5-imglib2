/**
 * Copyright (c) 2017-2018, Stephan Saalfeld, Philipp Hanslovsky, Igor Pisarev
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

import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.janelia.saalfeldlab.n5.GzipCompression;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.RawCompression;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.list.ListImg;
import net.imglib2.type.label.Label;
import net.imglib2.type.label.LabelMultisetEntry;
import net.imglib2.type.label.LabelMultisetEntryList;
import net.imglib2.type.label.LabelMultisetType;
import net.imglib2.type.label.LabelMultisetType.Entry;
import net.imglib2.util.Intervals;
import net.imglib2.view.Views;

public class N5LabelMultisetsTest {

	static private String testDirPath = System.getProperty("user.home") + "/tmp/n5-imglib2-test";

	static private String datasetName = "/test/group/dataset/label";

	static private long[] dimensions = new long[]{11, 22, 33};

	static private int[] blockSize = new int[]{5, 7, 9};

	static private RandomAccessibleInterval<LabelMultisetType> expectedImg;

	static private N5Writer n5;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {

		final File testDir = new File(testDirPath);
		testDir.mkdirs();
		if (!(testDir.exists() && testDir.isDirectory()))
			throw new IOException("Could not create test directory for HDF5Utils test.");

		n5 = new N5FSWriter(testDirPath);

		final Random rnd = new Random();

		final int numElements = (int) Intervals.numElements(dimensions);
		final List<LabelMultisetType> typeElements = new ArrayList<>();
		for (int i = 0; i < numElements; ++i)
		{
			final int numEntries = rnd.nextInt(10);
			final LabelMultisetEntryList entries = new LabelMultisetEntryList(numEntries);
			for (int j = 0; j < numEntries; ++j)
				entries.add(new LabelMultisetEntry(rnd.nextInt(10000), rnd.nextInt(100)));
			typeElements.add(new LabelMultisetType(entries));
		}
		expectedImg = new ListImg<>(typeElements, dimensions);
	}

	@AfterClass
	public static void rampDownAfterClass() throws Exception {

		Assert.assertTrue(n5.remove(""));
	}

	@Test
	public void testSaveAndOpen() throws InterruptedException, ExecutionException {

		try {
			N5LabelMultisets.saveLabelMultiset(expectedImg, n5, datasetName, blockSize, new GzipCompression());
			RandomAccessibleInterval<LabelMultisetType> loaded = N5LabelMultisets.openLabelMultiset(n5, datasetName);
			assertEquals(loaded);

			ExecutorService exec = Executors.newFixedThreadPool(4);
			N5LabelMultisets.saveLabelMultiset(expectedImg, n5, datasetName + "-1", blockSize, new GzipCompression(), exec);
			loaded = N5LabelMultisets.openLabelMultiset(n5, datasetName + "-1");
			assertEquals(loaded);
			exec.shutdown();

			exec = Executors.newFixedThreadPool(8);
			final int[] differentBlockSize = {6, 10, 3};
			N5LabelMultisets.saveLabelMultiset(loaded, n5, datasetName + "-2", differentBlockSize, new RawCompression(), exec);
			loaded = N5LabelMultisets.openLabelMultiset(n5, datasetName + "-2");
			assertEquals(loaded);
			exec.shutdown();

		} catch (final IOException e) {
			fail("Failed by I/O exception.");
			e.printStackTrace();
		}
	}

	private void assertEquals(final RandomAccessibleInterval<LabelMultisetType> actualImg) {

		Assert.assertTrue(Intervals.equals(expectedImg, actualImg));
		final Iterator<LabelMultisetType> expectedImgIterator = Views.flatIterable(expectedImg).iterator();
		final Iterator<LabelMultisetType> actualImgIterator = Views.flatIterable(actualImg).iterator();
		while (expectedImgIterator.hasNext() || actualImgIterator.hasNext()) {
			final LabelMultisetType expected = expectedImgIterator.next();
			final LabelMultisetType actual = actualImgIterator.next();
			Assert.assertEquals(expected.argMax(), actual.argMax());
			Assert.assertEquals(expected.size(), actual.size());
			Assert.assertEquals(expected.entrySet().size(), actual.entrySet().size());
			final Iterator<Entry<Label>> expectedEntriesIterator = expected.entrySet().iterator();
			final Iterator<Entry<Label>> actualEntriesIterator = actual.entrySet().iterator();
			while (expectedEntriesIterator.hasNext() || actualEntriesIterator.hasNext()) {
				final Entry<Label> expectedEntry = expectedEntriesIterator.next();
				final Entry<Label> actualEntry = actualEntriesIterator.next();
				Assert.assertEquals(expectedEntry.getElement().id(), actualEntry.getElement().id());
				Assert.assertEquals(expectedEntry.getCount(), actualEntry.getCount());
			}
		}
	}
}

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

import org.janelia.saalfeldlab.n5.N5FSReader;
import org.janelia.saalfeldlab.n5.N5Reader;

import bdv.util.Bdv;
import bdv.util.BdvFunctions;
import bdv.util.BdvOptions;
import bdv.util.volatiles.SharedQueue;
import bdv.util.volatiles.VolatileViews;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.cache.volatiles.CacheHints;
import net.imglib2.cache.volatiles.LoadingStrategy;
import net.imglib2.type.NativeType;

/**
 * Simple dataset viewer example to demonstrate how {@link N5Utils} may be used.
 *
 * Parameters
 *   first N5 container path, e.g. /home/you/test.n5
 *   followed by dataset paths, e.g. /data /more/data1 /more/data2 ...
 *
 * @author Stephan Saalfeld
 */
public class N5DatasetViewerExample {

	public static final void main(final String... args) throws IOException {

		mainT(args);
	}

	public static final <T extends NativeType<T>> void mainT(final String... args) throws IOException {

		final N5Reader n5Reader = new N5FSReader(args[0]);
		Bdv bdv = null;

		final int numProc = Math.max(1, Runtime.getRuntime().availableProcessors() / 2);
		final SharedQueue queue = new SharedQueue(numProc);
		final CacheHints cacheHints = new CacheHints(LoadingStrategy.VOLATILE, 0, true);

		for (int i = 1; i < args.length; ++i) {
			final String n5Dataset = args[i];
			if (n5Reader.datasetExists(n5Dataset)) {
				final BdvOptions options = bdv == null ? Bdv.options() : Bdv.options().addTo(bdv);
				final RandomAccessibleInterval<T> dataset = N5Utils.openVolatile(n5Reader, n5Dataset);
				bdv = BdvFunctions.show(
						VolatileViews.wrapAsVolatile(
								dataset,
								queue,
								cacheHints),
						n5Dataset,
						options);
			}
		}
	}
}

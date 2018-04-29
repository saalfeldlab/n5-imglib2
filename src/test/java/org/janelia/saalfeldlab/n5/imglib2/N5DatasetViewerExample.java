/**
 *
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

/**
 * Simple
 * @author Stephan Saalfeld &lt;saalfelds@janelia.hhmi.org&gt;
 *
 */
public class N5DatasetViewerExample {

	@SuppressWarnings("unchecked")
	public static final void main(final String... args) throws IOException {

		final N5Reader n5Reader = new N5FSReader(args[0]);
		Bdv bdv = null;

		final int numProc = Math.max(1, Runtime.getRuntime().availableProcessors() / 2);
		final SharedQueue queue = new SharedQueue(numProc);
		final CacheHints cacheHints = new CacheHints(LoadingStrategy.VOLATILE, 0, true);

		for (int i = 1; i < args.length; ++i) {
			final String n5Dataset = args[i];
			if (n5Reader.datasetExists(n5Dataset)) {
				final BdvOptions options = bdv == null ? Bdv.options() : Bdv.options().addTo(bdv);
				final RandomAccessibleInterval dataset = N5Utils.openVolatile(n5Reader, n5Dataset);
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

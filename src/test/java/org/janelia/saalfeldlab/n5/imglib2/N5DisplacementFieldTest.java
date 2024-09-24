package org.janelia.saalfeldlab.n5.imglib2;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Random;

import org.janelia.saalfeldlab.n5.GzipCompression;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.junit.Test;

import net.imglib2.FinalInterval;
import net.imglib2.FinalRealInterval;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.RealLocalizable;
import net.imglib2.RealPoint;
import net.imglib2.iterator.LocalizingRealIntervalIterator;
import net.imglib2.position.FunctionRealRandomAccessible;
import net.imglib2.realtransform.AffineTransform3D;
import net.imglib2.realtransform.DisplacementFieldTransform;
import net.imglib2.realtransform.RealTransform;
import net.imglib2.type.numeric.integer.IntType;
import net.imglib2.type.numeric.real.DoubleType;
import net.imglib2.view.composite.RealComposite;

public class N5DisplacementFieldTest {

	static private String testDirPath = System.getProperty("user.home") + "/tmp/n5-imglib2-test";

	@Test
	public void testDfieldSaveLoad() {

		final String destPath = testDirPath + "/dfield.n5";
		final String dataset = "/dfieldOffset";

		// make a continuous displacement field
		final long[] sz = new long[]{16, 16, 16};
		final FunctionRealRandomAccessible<RealComposite<DoubleType>> dfield = makeDisplacementFunction(sz);
		// final RealRandomAccessible< RealComposite< DoubleType > > dfield =
		// makeDisplacementArray( sz );
		final DisplacementFieldTransform transform = new DisplacementFieldTransform(dfield);

		final AffineTransform3D affine = new AffineTransform3D();
		final FinalInterval interval = new FinalInterval(sz);
		final double[] spacing = new double[]{10, 20, 30};
		final double[] offset = new double[]{5, 6, 7};
		final int[] blockSize = new int[]{32, 32, 32, 32};

		final FinalRealInterval itvl = new FinalRealInterval(
				offset,
				new double[]{
						offset[0] + spacing[0] * (sz[0] - 1),
						offset[1] + spacing[1] * (sz[1] - 1),
						offset[2] + spacing[2] * (sz[2] - 1)});

		// discretize it
		final RandomAccessibleInterval<DoubleType> rai = DisplacementFieldTransform.createDisplacementField(transform, interval, spacing, offset);

		N5FSWriter n5 = null;
		try {
			n5 = new N5FSWriter(destPath);

			// save it
			N5DisplacementField.save(n5, dataset, affine, rai, spacing, offset, blockSize, new GzipCompression(), new IntType(), 1e-5);

		} catch (final Exception e) {
			e.printStackTrace();
			fail("failed to save displacement field");
		}

		// load it
		try {
			final RealTransform readTransform = N5DisplacementField.open(n5, dataset, false);
			final RealPoint qTrue = new RealPoint(0, 0, 0);
			final RealPoint qXfm = new RealPoint(0, 0, 0);
			final LocalizingRealIntervalIterator it = new LocalizingRealIntervalIterator(itvl, spacing);
			while (it.hasNext()) {
				it.fwd();
				transform.apply(it, qTrue);
				readTransform.apply(it, qXfm);
				final double err = distance(qTrue, qXfm);
				assertTrue("error check", err < 1e-3);
			}
		} catch (final Exception e) {
			e.printStackTrace();
			fail("failed to load displacement field");
		}
	}

	public static FunctionRealRandomAccessible<RealComposite<DoubleType>> makeDisplacementFunction(final long[] sz) {

		final Random r = new Random();
		return new FunctionRealRandomAccessible<>(3,
				(x, v) -> {
					r.setSeed((long)Math.round(x.getDoublePosition(0) + sz[0] * x.getDoublePosition(1) + sz[0] * sz[1] * x.getDoublePosition(2)));
					v.setPosition(r.nextDouble(), 0);
					v.setPosition(r.nextDouble(), 1);
					v.setPosition(r.nextDouble(), 2);
				},
				() -> {
					return DoubleType.createVector(3);
				});
	}

	// public static RealRandomAccessible< RealComposite< DoubleType > >
	// makeDisplacementArray( long[] sz )
	// {
	// final Random rand = new Random( 184 );
	// final long N = Arrays.stream( sz ).reduce( 1, (x,y) -> { return x * y;}
	// );
	// final double[] data = new double[ sz.length * (int)N ];
	// for( int i = 0; i < data.length; i++ )
	// data[ i ] = rand.nextDouble();
	//
	// long[] dfieldSz = new long[ sz.length + 1 ];
	// dfieldSz[0] = sz.length;
	// for( int i = 1; i < dfieldSz.length; i++ )
	// dfieldSz[i] = sz[i-1];
	//
	// final ArrayImg< DoubleType, DoubleArray > img = ArrayImgs.doubles( data,
	// sz );
	// return Views.interpolate( Views.extendBorder( Views.collapseReal( img )),
	// new NLinearInterpolatorFactory<RealComposite<DoubleType>>());
	// }

	public static double distance(final RealLocalizable position1, final RealLocalizable position2) {

		double dist = 0;
		final int n = position1.numDimensions();
		for (int d = 0; d < n; ++d) {
			final double pos = position2.getDoublePosition(d) - position1.getDoublePosition(d);
			dist += pos * pos;
		}
		return Math.sqrt(dist);
	}
}

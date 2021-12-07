package net.imglib2.realtransform;

import static org.junit.Assert.*;

import org.junit.Test;

import net.imglib2.RealPoint;

public class StackedTransformTest {

	@Test
	public void testStackedTransform() {
	
		Scale2D s0 = new Scale2D( 5, 4 );
		Scale2D s1 = new Scale2D( 3, 2 );
		StackedRealTransform s = new StackedRealTransform( s0, s1 );

		assertEquals("src dims", 4, s.numSourceDimensions());
		assertEquals("tgt dims", 4, s.numTargetDimensions());

		final double[] xOrig = new double[] { 2, 4, 8, 16 };
		final double[] x = new double[] { 2, 4, 8, 16 };
		final double[] y = new double[] { 10, 16, 24, 32 };
		final RealPoint p = new RealPoint( x );

		s.apply(x, x);
		assertArrayEquals("apply array in place", y, x, 1e-9);

		s.apply(p, p);
		p.localize(x);
		assertArrayEquals("apply point in place", y, x, 1e-9);

		StackedInvertibleRealTransform si = new StackedInvertibleRealTransform( s0, s1 );
		p.setPosition(y);

		si.applyInverse(y, y);
		assertArrayEquals("apply inv array in place", xOrig, y, 1e-9);

		si.applyInverse(p,p);
		p.localize(y);
		assertArrayEquals("apply inv array in place", xOrig, y, 1e-9);

	}
}

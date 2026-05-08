package net.imglib2.realtransform;

import java.util.Arrays;

import org.junit.Test;

public class InvertibleCoordinateMappingTest {

	@Test
	public void testStackedTransform() {

		final int[] perm = new int[]{1, 2, 0};
		final RealInvertibleComponentMappingTransform xfm = new RealInvertibleComponentMappingTransform(perm);

		final double[] xOrig = new double[]{7, 5, 3};
		final double[] x = new double[]{7, 5, 3};
		final double[] y = new double[3];

		xfm.apply(x, y);
		System.out.println("x: " + Arrays.toString(x));
		System.out.println("y: " + Arrays.toString(y));

		xfm.applyInverse(y, x);
		System.out.println("x new: " + Arrays.toString(x));
	}
}

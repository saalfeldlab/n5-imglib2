package net.imglib2.realtransform;

import static org.junit.Assert.*;

import java.util.Arrays;

import org.junit.Test;

public class AxisSelectionTransformTest {

	@Test
	public void testStackedTransform() {
	
		Scale2D s0 = new Scale2D( 5, 4, 3 );
		
		int[] a01 = new int[] { 0, 1 };
		int[] a02 = new int[] { 0, 2 };
		int[] a12 = new int[] { 1, 2 };

		AxisSelectionTransform subTransform = new AxisSelectionTransform(s0, a02, false);
		
		double[] x = new double[] { 7, 5 };
		double[] y = new double[ 4 ];
		
		subTransform.apply(x, y);
		System.out.println( "x: " + Arrays.toString(x));
		System.out.println( "y: " + Arrays.toString(y));
		

	}

}

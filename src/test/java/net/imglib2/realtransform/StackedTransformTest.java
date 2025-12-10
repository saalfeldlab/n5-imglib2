/*-
 * #%L
 * N5 Cache Loader
 * %%
 * Copyright (C) 2017 - 2025 Philipp Hanslovsky, Stephan Saalfeld
 * %%
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDERS OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 * #L%
 */
package net.imglib2.realtransform;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

import net.imglib2.RealPoint;

public class StackedTransformTest {

	@Test
	public void testStackedTransform() {

		final Scale2D s0 = new Scale2D(5, 4);
		final Scale2D s1 = new Scale2D(3, 2);
		final StackedRealTransform s = new StackedRealTransform(s0, s1);

		assertEquals("src dims", 4, s.numSourceDimensions());
		assertEquals("tgt dims", 4, s.numTargetDimensions());

		final double[] xOrig = new double[]{2, 4, 8, 16};
		final double[] x = new double[]{2, 4, 8, 16};
		final double[] y = new double[]{10, 16, 24, 32};
		final RealPoint p = new RealPoint(x);

		s.apply(x, x);
		assertArrayEquals("apply array in place", y, x, 1e-9);

		s.apply(p, p);
		p.localize(x);
		assertArrayEquals("apply point in place", y, x, 1e-9);

		final StackedInvertibleRealTransform si = new StackedInvertibleRealTransform(s0, s1);
		p.setPosition(y);

		si.applyInverse(y, y);
		assertArrayEquals("apply inv array in place", xOrig, y, 1e-9);

		si.applyInverse(p, p);
		p.localize(y);
		assertArrayEquals("apply inv array in place", xOrig, y, 1e-9);
	}
}

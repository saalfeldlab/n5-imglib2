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

import java.util.Arrays;

import net.imglib2.RealLocalizable;
import net.imglib2.RealPositionable;

public class StackedInvertibleRealTransform extends StackedRealTransform implements InvertibleRealTransform {

	private final InvertibleRealTransform[] transforms;

	public StackedInvertibleRealTransform(final InvertibleRealTransform... transforms) {

		super(transforms);
		this.transforms = transforms;
	}

	@Override
	public InvertibleRealTransform copy() {

		return new StackedInvertibleRealTransform(
				Arrays.stream(transforms).map(InvertibleRealTransform::copy).toArray(InvertibleRealTransform[]::new));
	}

	@Override
	public void applyInverse(final double[] source, final double[] target) {

		int startSrc = 0;
		int startTgt = 0;
		for (final InvertibleRealTransform t : transforms) {

			System.arraycopy(target, startTgt, tmpTgt, 0, t.numTargetDimensions());
			t.applyInverse(tmpSrc, tmpTgt);
			System.arraycopy(tmpSrc, 0, source, startSrc, t.numSourceDimensions());

			startSrc += t.numSourceDimensions();
			startTgt += t.numTargetDimensions();
		}
	}

	@Override
	public void applyInverse(final RealPositionable source, final RealLocalizable target) {

		int startSrc = 0;
		int startTgt = 0;
		for (final InvertibleRealTransform t : transforms) {

			localizeFromIndex(target, tmpTgt, startTgt, t.numTargetDimensions());
			t.applyInverse(tmpSrc, tmpTgt);
			positionFromIndex(source, tmpSrc, startSrc, t.numSourceDimensions());

			startSrc += t.numSourceDimensions();
			startTgt += t.numTargetDimensions();
		}
	}

	@Override
	public InvertibleRealTransform inverse() {

		// TODO Auto-generated method stub
		return null;
	}
}

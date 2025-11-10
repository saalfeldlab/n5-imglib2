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

import net.imglib2.RealLocalizable;
import net.imglib2.RealPositionable;

public class RealComponentMappingTransform implements RealTransform {

	protected int numSourceDimensions;
	protected int numTargetDimensions;
	protected int[] components;

	public RealComponentMappingTransform(final int numSourceDimensions, final int[] components) {

		this.components = components;
		this.numSourceDimensions = numSourceDimensions;
		this.numTargetDimensions = components.length;
	}

	@Override
	public void apply(final double[] source, final double[] target) {

		assert source.length >= numTargetDimensions;
		assert target.length >= numTargetDimensions;

		for (int d = 0; d < numTargetDimensions; ++d)
			target[d] = source[components[d]];
	}

	@Override
	public void apply(final RealLocalizable source, final RealPositionable target) {

		assert source.numDimensions() >= numTargetDimensions;
		assert target.numDimensions() >= numTargetDimensions;

		for (int d = 0; d < numTargetDimensions; ++d)
			target.setPosition(source.getDoublePosition(components[d]), d);
	}

	@Override
	public RealComponentMappingTransform copy() {

		return new RealComponentMappingTransform(numSourceDimensions, components);
	}

	@Override
	public int numSourceDimensions() {

		return numSourceDimensions;
	}

	@Override
	public int numTargetDimensions() {

		return numTargetDimensions;
	}
}

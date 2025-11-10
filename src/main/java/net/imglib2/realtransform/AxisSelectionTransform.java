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

public class AxisSelectionTransform implements RealTransform {

	private final RealTransform transform;

	private final int[] axes;

	private final boolean unProject;

	private final RealInvertibleComponentMappingTransform permutation;

	private final RealTransformSequence seq;

	public AxisSelectionTransform(
			final RealTransform transform,
			final int[] axes,
			final int nd,
			final boolean unProject) {

		this.transform = transform;
		this.axes = axes;
		this.unProject = unProject;
		permutation = new RealInvertibleComponentMappingTransform(axes);

		seq = new RealTransformSequence();
		seq.add(permutation);
		seq.add(transform);
		if (unProject)
			seq.add(permutation.inverse());
	}

	public AxisSelectionTransform(final RealTransform transform, final int[] axes, final boolean unProject) {

		this(transform, axes, Arrays.stream(axes).max().getAsInt(), unProject);
	}

	@Override
	public int numSourceDimensions() {

		return permutation.numDimensions();
	}

	@Override
	public int numTargetDimensions() {

		return unProject ? numSourceDimensions() : transform.numTargetDimensions();
	}

	@Override
	public void apply(final double[] source, final double[] target) {

		seq.apply(source, target);
	}

	@Override
	public void apply(final RealLocalizable source, final RealPositionable target) {

		seq.apply(source, target);
	}

	@Override
	public RealTransform copy() {

		return new AxisSelectionTransform(transform, axes, unProject);
	}
}

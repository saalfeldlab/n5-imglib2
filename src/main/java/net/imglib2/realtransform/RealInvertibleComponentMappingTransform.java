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
import java.util.TreeSet;
import java.util.stream.IntStream;

import net.imglib2.RealLocalizable;
import net.imglib2.RealPoint;
import net.imglib2.RealPositionable;

public class RealInvertibleComponentMappingTransform extends RealComponentMappingTransform implements InvertibleRealTransform, AffineGet {

	protected final int[] inverseComponent;

	protected final RealPoint dpt;

	public RealInvertibleComponentMappingTransform(final int[] componentIn) {

		this(Arrays.stream(componentIn).max().getAsInt() + 1, componentIn);
	}

	public RealInvertibleComponentMappingTransform(final int nd, final int[] componentIn) {

		super(nd, checkAndFillComponent(componentIn, nd));

		inverseComponent = new int[nd];
		for (int i = 0; i < nd; i++) {
			inverseComponent[components[i]] = i;
		}
		dpt = new RealPoint(components.length);
	}

	private static int[] checkAndFillComponent(final int[] component, int nd) {

		final TreeSet<Integer> sortedIndexes = new TreeSet<Integer>();
		for (int i = 0; i < component.length; i++) {
			sortedIndexes.add(component[i]);
		}
		final int max = sortedIndexes.last();
		if (max > nd - 1)
			nd = max + 1;

		if (component.length == nd)
			return component;
		else
			return buildNewComponent(component, sortedIndexes, nd);
	}

	private static int[] buildNewComponent(final int[] component, final TreeSet<Integer> sortedIndexes, final int nd) {

		final TreeSet<Integer> missingIndexes = new TreeSet<>();
		IntStream.range(0, nd).forEach(i -> missingIndexes.add(i));
		missingIndexes.removeAll(sortedIndexes);

		final int[] compOut = new int[nd];
		System.arraycopy(component, 0, compOut, 0, component.length);
		int i = component.length;
		for (; i < nd; i++)
			compOut[i] = missingIndexes.pollFirst();

		return compOut;
	}

	@Override
	public void applyInverse(final double[] source, final double[] target) {

		assert source.length >= numTargetDimensions;
		assert target.length >= numTargetDimensions;

		for (int d = 0; d < numTargetDimensions; ++d)
			source[d] = target[inverseComponent[d]];
	}

	@Override
	public void applyInverse(final RealPositionable source, final RealLocalizable target) {

		assert source.numDimensions() >= numTargetDimensions;
		assert target.numDimensions() >= numTargetDimensions;

		for (int d = 0; d < numTargetDimensions; ++d)
			source.setPosition(target.getDoublePosition(inverseComponent[d]), d);
	}

	@Override
	public RealInvertibleComponentMappingTransform copy() {

		return new RealInvertibleComponentMappingTransform(components);
	}

	@Override
	public RealInvertibleComponentMappingTransform inverse() {

		return new RealInvertibleComponentMappingTransform(inverseComponent);
	}

	@Override
	public int numDimensions() {

		return components.length;
	}

	@Override
	public double get(final int row, final int column) {

		return components[row] == column ? 1 : 0;
	}

	@Override
	public double[] getRowPackedCopy() {

		final int n = components.length;
		final double[] mtx = new double[n * (n + 1)];
		for (int i = 0; i < n; i++)
			mtx[i + (n + 1) * components[i]] = 1;

		return mtx;
	}

	@Override
	public RealLocalizable d(final int d) {

		for (int i = 0; i < components.length; i++)
			dpt.setPosition(get(i, d), i);

		return dpt;
	}
}

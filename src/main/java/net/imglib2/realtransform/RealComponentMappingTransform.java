package net.imglib2.realtransform;

import net.imglib2.RealLocalizable;
import net.imglib2.RealPositionable;
import net.imglib2.transform.integer.ComponentMappingTransform;

public class RealComponentMappingTransform implements RealTransform {

	protected int numSourceDimensions;
	protected int numTargetDimensions;
	protected int[] components;

	public RealComponentMappingTransform( final int numSourceDimensions, final int[] components ) {
		this.components = components;
		this.numSourceDimensions = numSourceDimensions;
		this.numTargetDimensions = components.length;
	}

	@Override
	public void apply(double[] source, double[] target) {
		assert source.length >= numTargetDimensions;
		assert target.length >= numTargetDimensions;

		for ( int d = 0; d < numTargetDimensions; ++d )
			target[ d ] = source[ components[ d ] ];
	}

	@Override
	public void apply(RealLocalizable source, RealPositionable target) {
		assert source.numDimensions() >= numTargetDimensions;
		assert target.numDimensions() >= numTargetDimensions;

		for ( int d = 0; d < numTargetDimensions; ++d )
			target.setPosition( source.getDoublePosition( components[ d ] ), d );
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

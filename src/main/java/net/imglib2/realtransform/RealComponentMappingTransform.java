package net.imglib2.realtransform;

import net.imglib2.RealLocalizable;
import net.imglib2.RealPositionable;
import net.imglib2.transform.integer.ComponentMappingTransform;

public class RealComponentMappingTransform extends ComponentMappingTransform implements RealTransform {

	public RealComponentMappingTransform( final int[] component ) {
		super( component );
	}

	@Override
	public void apply(double[] source, double[] target) {
		assert source.length >= numTargetDimensions;
		assert target.length >= numTargetDimensions;

		for ( int d = 0; d < numTargetDimensions; ++d )
			target[ d ] = source[ component[ d ] ];
	}

	@Override
	public void apply(RealLocalizable source, RealPositionable target) {
		assert source.numDimensions() >= numTargetDimensions;
		assert target.numDimensions() >= numTargetDimensions;

		for ( int d = 0; d < numTargetDimensions; ++d )
			target.setPosition( source.getDoublePosition( component[ d ] ), d );	
	}

	@Override
	public RealComponentMappingTransform copy() {
		return new RealComponentMappingTransform(component);
	}

}

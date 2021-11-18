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

	public RealInvertibleComponentMappingTransform( final int[] componentIn ) {
		this(componentIn, Arrays.stream(componentIn).max().getAsInt() + 1);
	}
	
	public RealInvertibleComponentMappingTransform( final int[] componentIn, final int nd ) {
		super( checkAndFillComponent( componentIn, nd ) );

		inverseComponent = new int[ nd ];
		for( int i  = 0; i < nd; i++ ) {
			inverseComponent[component[i]] = i;
		}
		dpt = new RealPoint( component.length );
	}
	
	private static int[] checkAndFillComponent( int[] component, int nd )
	{
		final TreeSet<Integer> sortedIndexes = new TreeSet<Integer>();
		for( int i  = 0; i < component.length; i++ ) {
			sortedIndexes.add(component[i]);
		}
		int max = sortedIndexes.last();
		if( max > nd -1 )
			nd = max + 1;

		if( component.length == nd )
			return component;
		else 
			return buildNewComponent( component, sortedIndexes, nd );
	}

	private static int[] buildNewComponent( int[] component, TreeSet<Integer> sortedIndexes, int nd )
	{
		final TreeSet<Integer> missingIndexes = new TreeSet<>();
		IntStream.range(0, nd).forEach( i -> missingIndexes.add( i ));
		missingIndexes.removeAll(sortedIndexes);

		int[] compOut = new int[ nd ];
		System.arraycopy( component, 0, compOut, 0, component.length );
		int i = component.length;
		for( ; i < nd; i++ )
			compOut[ i ] = missingIndexes.pollFirst();

		return compOut;
	}

	@Override
	public void applyInverse(double[] source, double[] target) {
		assert source.length >= numTargetDimensions;
		assert target.length >= numTargetDimensions;

		for ( int d = 0; d < numTargetDimensions; ++d )
			target[ d ] = source[ inverseComponent[ d ] ];	
	}

	@Override
	public void applyInverse(RealPositionable source, RealLocalizable target) {
		assert source.numDimensions() >= numTargetDimensions;
		assert target.numDimensions() >= numTargetDimensions;

		for ( int d = 0; d < numTargetDimensions; ++d )
			source.setPosition( target.getDoublePosition( inverseComponent[ d ] ), d );	
	}

	@Override
	public RealInvertibleComponentMappingTransform copy() {
		return new RealInvertibleComponentMappingTransform(component);
	}

	@Override
	public RealInvertibleComponentMappingTransform inverse() {
		return new RealInvertibleComponentMappingTransform(inverseComponent);
	}

	@Override
	public int numDimensions() {
		return component.length;
	}

	@Override
	public double get(int row, int column) {
		return component[row] == column ? 1 : 0;
	}

	@Override
	public double[] getRowPackedCopy() {
		final int n = component.length;
		final double[] mtx = new double[ n * ( n + 1)];
		for( int i = 0; i < n; i++ )
			mtx[i + (n + 1) * component[i]] = 1;

		return mtx;
	}

	@Override
	public RealLocalizable d(int d) {
		for( int i = 0; i < component.length; i++ )
			dpt.setPosition(get(i, d), i);

		return dpt;
	}

}

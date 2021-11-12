package org.janelia.saalfeldlab.n5.metadata.canonical;

import java.util.Arrays;
import java.util.stream.IntStream;

import org.janelia.saalfeldlab.n5.metadata.axes.Axis;
import org.janelia.saalfeldlab.n5.metadata.axes.AxisUtils;
import org.janelia.saalfeldlab.n5.metadata.transforms.ScaleSpatialTransform;
import org.janelia.saalfeldlab.n5.metadata.transforms.SpatialTransform;
import org.janelia.saalfeldlab.n5.translation.JqUtils;

import com.google.gson.Gson;

/**
 * A data structure corresponding to Proposal D here:
 * https://github.com/saalfeldlab/n5-ij/wiki/Transformation-spec-proposal-for-NGFF#proposal-d
 * 
 * @author John Bogovic
 */
public class TransformsWithAxesMetadata {

	private final TransformsWithAxes[] transforms;

	public TransformsWithAxesMetadata(TransformsWithAxes[] transforms) {
		this.transforms = transforms;
	}

	public TransformsWithAxes[] getTransforms() {
		return transforms;
	}

	public static IndexedAxis[] axesFromLabels(String[] labels, String[] units) {
		final String[] types = AxisUtils.getDefaultTypes(labels);

		int N = labels.length;
		IndexedAxis[] axesIdx = new IndexedAxis[N];
		for (int i = 0; i < N; i++) {
			axesIdx[i] = new IndexedAxis(types[i], labels[i], units[i], i);
		}
		return axesIdx;
	}

	public static IndexedAxis[] axesFromLabels( String[] labels, String unit ) {
		String[] units = new String[ labels.length ];
		Arrays.fill( units, unit );
		return axesFromLabels( labels, units );
	}

	public static IndexedAxis[] axesFromLabels(String... labels) {
		return axesFromLabels(labels, "none");
	}

	public static IndexedAxis[] dataAxes( int[] indexes ) {
		return Arrays.stream(indexes).mapToObj( i -> dataAxis(i) ).toArray( IndexedAxis[]::new );
	}
	
	public static IndexedAxis[] dataAxes( int N ) {
		return IntStream.range(0, N).mapToObj( i -> dataAxis(i) ).toArray( IndexedAxis[]::new );
	}

	public static IndexedAxis[] dataAxes( int start, int endInclusive ) {
		return IntStream.rangeClosed(start, endInclusive).mapToObj( i -> dataAxis(i) ).toArray( IndexedAxis[]::new );
	}

	public static IndexedAxis dataAxis(int i) {
		return new IndexedAxis("data", String.format("dim_%d", i), "none", i);
	}
	
	public static TransformsWithAxes transform( SpatialTransform transform,
			String... outputLabels ) {
		final IndexedAxis[] inputs = dataAxes( outputLabels.length );
		final IndexedAxis[] outputs = axesFromLabels( outputLabels );
		return new TransformsWithAxes( transform, inputs, outputs );
	}

	public static class TransformsWithAxes {

		public final SpatialTransform transform;
		public final IndexedAxis[] inputs;
		public final IndexedAxis[] outputs;

		public TransformsWithAxes(SpatialTransform transform, IndexedAxis[] inputs, IndexedAxis[] outputs) {
			this.transform = transform;
			this.inputs = inputs;
			this.outputs = outputs;
		}
	}

	public static class IndexedAxis extends Axis {

		private int index;

		public IndexedAxis(final String type, final String label, final String unit, final int index) {
			super(type, label, unit);
			this.index = index;
		}

		public int getIndex() {
			return index;
		}
	}
	
	public static void main( String[] args ) {

		final Gson gson = JqUtils.buildGson(null);
		final ScaleSpatialTransform xfm = new ScaleSpatialTransform( new double[] {2, 3, 4});

////		IndexedAxis[] inputs = dataAxes( new int[]{0,1,2});
////		IndexedAxis[] inputs = dataAxes( 0, 2 );
//		IndexedAxis[] inputs = dataAxes( 3 );
//		IndexedAxis[] outputs = axesFromLabels( "x", "y", "z" );
//
//		System.out.println( gson.toJson(inputs));
//		System.out.println( "" );
//		System.out.println( gson.toJson(outputs));

		TransformsWithAxes xfmAxis = transform(xfm, "x", "y", "z" );
		System.out.println( gson.toJson( xfmAxis ));

	}
}

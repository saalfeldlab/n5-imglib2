package org.janelia.saalfeldlab.n5.metadata.axisTransforms;

import org.janelia.saalfeldlab.n5.metadata.axes.IndexedAxis;
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

	public static TransformsWithAxes transform( SpatialTransform transform,
			String... outputLabels ) {
		final IndexedAxis[] inputs = IndexedAxis.dataAxes( outputLabels.length );
		final IndexedAxis[] outputs = IndexedAxis.axesFromLabels( outputLabels );
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

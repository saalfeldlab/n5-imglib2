package org.janelia.saalfeldlab.n5.metadata.axisTransforms;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Set;
import java.util.stream.IntStream;

import org.janelia.saalfeldlab.n5.N5FSReader;
import org.janelia.saalfeldlab.n5.metadata.axes.IndexedAxisMetadata;
import org.janelia.saalfeldlab.n5.metadata.transforms.SpatialTransform;
import org.janelia.saalfeldlab.n5.translation.JqUtils;

import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;

import net.imglib2.realtransform.RealTransform;
import net.imglib2.realtransform.RealTransformSequence;
import net.imglib2.realtransform.StackedRealTransform;

/**
 * A data structure corresponding to Proposal D here:
 * https://github.com/saalfeldlab/n5-ij/wiki/Transformation-spec-proposal-for-NGFF#proposal-d
 * 
 * @author John Bogovic
 */
public class TransformAxisMetadata  implements SpatialTransform, IndexedAxisMetadata  {

	public static final RealTransform IDENTITY = new RealTransformSequence();

	public TransformAxes[] transforms;

	private transient HashMap<Integer, Integer> idx2Xfm;

	public transient String[] labels;
	public transient String[] types;
	public transient String[] units;

	private TransformAxisMetadata( TransformAxes[] transforms ) {

		this.transforms = transforms;
		setDefaults();

		idx2Xfm = indexToTransform( transforms );
		int maxIdx = idx2Xfm.entrySet().stream().mapToInt( e -> e.getKey() ).max().getAsInt();
		
		int N = maxIdx + 1;
		labels = new String[ N ];
		types = new String[ N ];
		units = new String[ N ];

		for (TransformAxes ta : transforms) {
			for (int i = 0; i < ta.getAxisLabels().length; i++) {
				int idx = ta.getIndexes()[i];
				labels[idx] = ta.getAxisLabels()[i];
				types[idx] = ta.getAxisTypes()[i];
				units[idx] = ta.getUnits()[i];
			}
		}
	}

	protected void setDefaults() {
		int firstIndex = 0;
		for (TransformAxes ta : transforms) {
			ta.setDefaults(firstIndex);
			firstIndex += ta.getOutputAxes().length;
		}
	}

	public static TransformAxisMetadata build( TransformAxes... transforms ) {
		if( check( transforms ))
			return new TransformAxisMetadata( transforms );
		else
			return null;
	}

	public static HashMap<Integer,Integer> transformToIndexes( TransformAxes[] transforms ) {
		 final HashMap<Integer,Integer> transformToIndex = new HashMap<>();
		 for( int i = 0; i < transforms.length; i++ )
			 for( int j : transforms[i].getIndexes())
				 transformToIndex.put(i, j);

		 return transformToIndex;
	}
	
	public static HashMap<Integer,Integer> indexToTransform( TransformAxes[] transforms ) {
		 final HashMap<Integer,Integer> indexToTransform = new HashMap<>();
		 for( int i = 0; i < transforms.length; i++ )
			 for( int j : transforms[i].getIndexes())
				 indexToTransform.put(j, i);

		 return indexToTransform;
	}

	public static boolean check(TransformAxes[] transforms) {
		final HashMap<Integer, Integer> indexToTransform = new HashMap<>();
		for (int i = 0; i < transforms.length; i++) {
			for (int j : transforms[i].getIndexes()) {
				if (indexToTransform.containsKey(j))
					return false;
				else
					indexToTransform.put(j, i);
			}
		}

		final Set<Integer> idxSet = indexToTransform.keySet();
		return IntStream.range(0, idxSet.size()).allMatch(i -> idxSet.contains(i));
	}

	@Override
	public String[] getAxisLabels() {
		return labels;
	}

	@Override
	public String[] getAxisTypes() {
		return types;
	}

	@Override
	public String[] getUnits() {
		return units;
	}

	@Override
	public RealTransform getTransform() {

		// returns the first transformation where all axes are spatial dimensions or the identity otherwise
		return Arrays.stream( transforms ).filter( t ->
				Arrays.stream(t.getAxisTypes()).allMatch( u -> u.equals("space")))
			.map( t -> t.getTransform() )
			.findFirst()
			.orElse( IDENTITY );
	}

	public RealTransform getStackedTransform() {
		return new StackedRealTransform(
			Arrays.stream(transforms).map( TransformAxes::getTransform ).toArray( RealTransform[]::new ));
	}

	public RealTransform getTransform( int i ) {
		return transforms[i].getTransform();
	}

	public TransformAxes[] getTransformAxes() {
		return transforms;
	}

	public static void main( String[] args ) throws JsonSyntaxException, JsonIOException, IOException {

//		ScaleSpatialTransform spaceXfm = new ScaleSpatialTransform( new double[] {2, 3, 4 });
//		TransformAxes space = new TransformAxes( spaceXfm, "x","y","z");
//		TransformAxes channel = new TransformAxes( new int[]{3}, "c" );
//
//		TransformAxisMetadata tam = TransformAxisMetadata.build( space, channel );
//
//		Arrays.stream(tam.getAxisLabels()).forEach(System.out::println);
//		System.out.println("");
//		Arrays.stream(tam.getAxisTypes()).forEach(System.out::println);
//		System.out.println("");
//		Arrays.stream(tam.getUnits()).forEach(System.out::println);
//
//		System.out.println( "");
//		System.out.println( Arrays.toString( space.getUnits()));
////		System.out.println( Arrays.stream( space.getUnits()).allMatch( u -> u.equals("space")));
//
//		RealTransform spatialTransform = tam.getTransform();
//		System.out.println( spatialTransform );


		N5FSReader n5 = new N5FSReader( "/groups/saalfeld/home/bogovicj/dev/n5/n5-imglib2-translation/src/test/resources/canonical.n5", JqUtils.gsonBuilder(null));
		String dataset = "/transformAxis/czyx_short";
		
		TransformAxisMetadata tam = n5.getAttribute(dataset, "transformMetadata", TransformAxisMetadata.class );
		tam.setDefaults();
		System.out.println( tam );

	}

}

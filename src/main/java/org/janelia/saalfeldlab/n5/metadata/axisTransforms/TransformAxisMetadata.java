package org.janelia.saalfeldlab.n5.metadata.axisTransforms;

import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Set;
import java.util.stream.IntStream;

import org.janelia.saalfeldlab.n5.metadata.axes.IndexedAxisMetadata;
import org.janelia.saalfeldlab.n5.metadata.transforms.ScaleSpatialTransform;
import org.janelia.saalfeldlab.n5.metadata.transforms.SpatialTransform;

import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;

import net.imglib2.realtransform.RealTransform;
import net.imglib2.realtransform.StackedRealTransform;

public class TransformAxisMetadata  implements SpatialTransform, IndexedAxisMetadata  {

	public TransformAxes[] transforms;
	private HashMap<Integer, Integer> idx2Xfm;

	public String[] labels;
	public String[] types;
	public String[] units;

	private TransformAxisMetadata( TransformAxes[] transforms ) {
		idx2Xfm = indexToTransform( transforms );
		int maxIdx = idx2Xfm.entrySet().stream().mapToInt( e -> e.getKey() ).max().getAsInt();
		
		int N = maxIdx + 1;
		labels = new String[ N ];
		types = new String[ N ];
		units = new String[ N ];

		for (TransformAxes ta : transforms)
			for (int i = 0; i < ta.getAxisLabels().length; i++) {
				int idx = ta.getIndexes()[i];
				labels[idx] = ta.getAxisLabels()[i];
				types[idx] = ta.getAxisTypes()[i];
				units[idx] = ta.getUnits()[i];
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
		return new StackedRealTransform(
			Arrays.stream(transforms).map( TransformAxes::getTransform ).toArray( RealTransform[]::new ));
	}

	public RealTransform getTransform( int i ) {
		return transforms[i].getTransform();
	}

	public static void main( String[] args ) throws JsonSyntaxException, JsonIOException, FileNotFoundException {

		ScaleSpatialTransform spaceXfm = new ScaleSpatialTransform( new double[] {2, 3, 4 });
		TransformAxes space = new TransformAxes( spaceXfm, "x","y","z");
		TransformAxes channel = new TransformAxes( new int[]{3}, "c" );

		TransformAxisMetadata tam = TransformAxisMetadata.build( space, channel );
		
		Arrays.stream(tam.getAxisLabels()).forEach(System.out::println);
		System.out.println("");
		Arrays.stream(tam.getAxisTypes()).forEach(System.out::println);
		System.out.println("");
		Arrays.stream(tam.getUnits()).forEach(System.out::println);
	}

}

package org.janelia.saalfeldlab.n5.metadata.axes;

import java.util.Arrays;
import java.util.stream.IntStream;

public class IndexedAxis extends Axis{

	private final int index;

	public IndexedAxis( final String type, final String label, final String unit, final int index )
	{
		super( type, label, unit );
		this.index = index;
	}

	public int getIndex() {
		return index;
	}


	public static IndexedAxis[] axesFromLabels(String[] labels, String[] units, int[] indexes ) {
		final String[] types = AxisUtils.getDefaultTypes(labels);

		int N = labels.length;
		IndexedAxis[] axesIdx = new IndexedAxis[N];
		for (int i = 0; i < N; i++) {
			axesIdx[i] = new IndexedAxis(types[i], labels[i], units[i], indexes[i]);
		}
		return axesIdx;
	}

	public static IndexedAxis[] axesFromLabels( String[] labels, String unitIn ) {
		String unit = unitIn == null ? "none" : unitIn;
		String[] units = new String[ labels.length ];
		Arrays.fill( units, unit );
		return axesFromLabels(labels, units, IntStream.range(0, labels.length).toArray());
	}

	public static IndexedAxis[] axesFromLabels( String[] labels, int[] indexes, String unitIn ) {
		String unit = unitIn == null ? "none" : unitIn;
		String[] units = new String[ labels.length ];
		Arrays.fill( units, unit );
		return axesFromLabels( labels, units, indexes );
	}

	public static IndexedAxis[] axesFromLabels(String... labels) {
		return axesFromLabels(labels, "none");
	}

	public static IndexedAxis defaultAxis(int i, String unit ) {
		return new IndexedAxis("unknown", Integer.toString(i), unit, i );
	}

	public static IndexedAxis[] axesFromIndexes(int[] indexes) {
		return Arrays.stream(indexes).mapToObj(i -> defaultAxis(i, "none")).toArray(IndexedAxis[]::new);
	}

	public static IndexedAxis[] axesFromIndexes(int[] indexes, String unit ) {
		return Arrays.stream(indexes).mapToObj(i -> defaultAxis(i, unit)).toArray(IndexedAxis[]::new);
	}

	public static IndexedAxis[] dataAxes(int[] indexes) {
		return Arrays.stream(indexes).mapToObj(i -> dataAxis(i)).toArray(IndexedAxis[]::new);
	}

	public static IndexedAxis[] dataAxes(int N) {
		return IntStream.range(0, N).mapToObj(i -> dataAxis(i)).toArray(IndexedAxis[]::new);
	}

	public static IndexedAxis[] dataAxes(int start, int endInclusive) {
		return IntStream.rangeClosed(start, endInclusive).mapToObj(i -> dataAxis(i)).toArray(IndexedAxis[]::new);
	}

	public static IndexedAxis dataAxis(int i) {
		return new IndexedAxis("data", String.format("dim_%d", i), "none", i);
	}
	
}

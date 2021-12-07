package org.janelia.saalfeldlab.n5.translation;

import java.util.ArrayList;
import java.util.function.Predicate;

import org.janelia.saalfeldlab.n5.container.ContainerMetadataNode;

import com.google.gson.Gson;

public class PiecewiseContainerTranslation implements ContainerTranslation {

	private ArrayList<OptionalTranslation> list;

	public PiecewiseContainerTranslation() {
		list = new ArrayList<>();
	}

	@Override
	public ContainerMetadataNode apply(ContainerMetadataNode x) {
		for( OptionalTranslation t : list )
			if( t.predicate.test(x))
				return t.translation.apply(x);

		return x;
	}
	
	public void add( Predicate<ContainerMetadataNode> pred, 
			ContainerTranslation translation ) {
		list.add(new OptionalTranslation(pred, translation));
	}
	
	private static class OptionalTranslation {
		public Predicate<ContainerMetadataNode> predicate;
		public ContainerTranslation translation;
		
		public OptionalTranslation( Predicate<ContainerMetadataNode> predicate, 
				ContainerTranslation translation)
		{
			this.predicate = predicate;
			this.translation = translation;
		}
	}

}

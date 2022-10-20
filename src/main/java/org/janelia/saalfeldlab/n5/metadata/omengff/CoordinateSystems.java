package org.janelia.saalfeldlab.n5.metadata.omengff;

import java.util.Optional;

import ome.ngff.axes.CoordinateSystem;

public class CoordinateSystems
{
	private CoordinateSystem[] coordinateSystems;

	public CoordinateSystems( CoordinateSystem[] coordinateSystems ) {
		this.coordinateSystems = coordinateSystems;
	}
	
	public Optional<CoordinateSystem> get( String name ){
		for( CoordinateSystem cs : coordinateSystems)
		{
			if( cs.getName().equals( name ))
				return Optional.of( cs );
		}
		return Optional.empty();
	}
	
	public CoordinateSystem[] get()
	{
		return coordinateSystems;
	}

}

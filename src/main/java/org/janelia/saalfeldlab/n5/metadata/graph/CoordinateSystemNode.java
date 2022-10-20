package org.janelia.saalfeldlab.n5.metadata.graph;

import java.util.ArrayList;
import java.util.List;

import org.janelia.saalfeldlab.n5.metadata.omengff.NgffCoordinateTransformation;

import ome.ngff.axes.CoordinateSystem;
import ome.ngff.transformations.CoordinateTransformation;

/**
 * A node in a {@link TransformGraph}.
 * 
 * Edges are directed with this node as their base.
 * 
 * @author John Bogovic
 */
public class CoordinateSystemNode
{
	private final CoordinateSystem space;

	private final List< NgffCoordinateTransformation<?> > edges;

	public CoordinateSystemNode( final CoordinateSystem space )
	{
		this( space, new ArrayList< NgffCoordinateTransformation<?> >() );
	}

	public CoordinateSystemNode( final CoordinateSystem space, List< NgffCoordinateTransformation<?> > edges )
	{
		this.space = space;
		this.edges = edges;
	}

	public CoordinateSystem node()
	{
		return space;
	}

	public List< NgffCoordinateTransformation< ? > > edges()
	{
		return edges;
	}

	@Override
	public boolean equals( Object other )
	{
		return space.equals(other);
	}

}

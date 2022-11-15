package org.janelia.saalfeldlab.n5.metadata.graph;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.metadata.omengff.InverseNgffCoordinateTransformation;
import org.janelia.saalfeldlab.n5.metadata.omengff.NgffCoordinateTransformation;
import org.janelia.saalfeldlab.n5.metadata.omengff.NgffIdentityTransformation;
import org.janelia.saalfeldlab.n5.metadata.omengff.NgffInvertibleCoordinateTransformation;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import net.imglib2.realtransform.InvertibleRealTransform;
import ome.ngff.axes.CoordinateSystem;
import ome.ngff.transformations.CoordinateTransformation;
import ome.ngff.transformations.CoordinateTransformationAdapter;

public class TransformGraph
{
	public final Gson gson;

	private final ArrayList< NgffCoordinateTransformation<?> > transforms;
	
	private HashMap< String, CoordinateSystem > nameToCoordinateSystem;

	private HashMap< String, CoordinateSystemNode > nameToNodes;

	private Set<CoordinateSystem > coordinateSystems;

	public TransformGraph() {
		transforms = new ArrayList<>();
		coordinateSystems = new HashSet<>();
		nameToCoordinateSystem = new HashMap<>();
		nameToNodes = new HashMap<>();

		final GsonBuilder gb = new GsonBuilder();
		gb.registerTypeAdapter(CoordinateTransformation.class, new CoordinateTransformationAdapter() );
		gson = gb.create();
	}
	
	public TransformGraph( N5Reader n5, String dataset )
	{
		this();
		try
		{
			// add coordinate systems
			final CoordinateSystem[] css = n5.getAttribute( dataset, CoordinateSystem.KEY, CoordinateSystem[].class );
			if( css != null )
				for( CoordinateSystem cs : css )
					addCoordinateSystem( cs );
			
			if( n5.datasetExists( dataset ))
			{
				DatasetAttributes attrs = n5.getDatasetAttributes( dataset );
				addCoordinateSystem( CoordinateSystem.defaultArray( dataset, attrs.getNumDimensions() ));
			}

			final CoordinateTransformation[] cts = n5.getAttribute( dataset, CoordinateTransformation.KEY, CoordinateTransformation[].class );
			if( cts != null )
				for( CoordinateTransformation ct : cts )
					addTransform( ct );
		}
		catch ( IOException e )
		{
			e.printStackTrace();
		}
	}

//	public TransformGraph( List<CoordinateTransform<?>> transforms, final Spaces spaces ) {
//
//		gson = SpacesTransforms.buildGson();
//		this.spaces = spaces;
//		this.transforms = new ArrayList<>();
////		this.transforms.addAll(transforms);
////		this.transforms = transforms = new ArrayList<>();;
//		inferSpacesFromAxes();
//
//		spacesToNodes = new HashMap< Space, SpaceNode >();
//		for( CoordinateTransform<?> t : transforms )
//		{
//			addTransform(t);
//
////			final Space src = getInputSpace( t );
////			if( spacesToNodes.containsKey( src ))
////				spacesToNodes.get( src ).edges().add( t );
////			else
////			{
////				SpaceNode node = new SpaceNode( src );
////				node.edges().add( t );
////				spacesToNodes.put( src, node );
////			}
//		}
//		updateTransforms();
//	}

//	public TransformGraph( List< CoordinateTransform<?> > transforms, final List<Space> spacesIn ) {
//		this( transforms, new Spaces(spacesIn) );
//	}

	public CoordinateSystem getCoordinateSystem( String name )
	{
		return nameToCoordinateSystem.get( name );
	}

	public ArrayList< NgffCoordinateTransformation< ? > > getTransforms() {
		return transforms;
	}

	public Set< CoordinateSystem > getCoordinateSystems() {
		return coordinateSystems;
	}
	
	public boolean hasSpace( String name )
	{
		return nameToCoordinateSystem.containsKey( name );
	}

	public Optional<NgffCoordinateTransformation<?>> getTransform( String name ) {
		return transforms.stream().filter( x -> x.getName().equals(name)).findAny();
	}

	public CoordinateSystem getInput( CoordinateTransformation t ) {
		return getCoordinateSystem(t.getInput());
	}

	public CoordinateSystem getOutput( CoordinateTransformation t ) {
		return getCoordinateSystem(t.getOutput());
	}

	public void addTransform( CoordinateTransformation t ) {
		addTransform( t, true );
	}

	public void addTransform( CoordinateTransformation t, boolean addInverse ) {
		addTransform( NgffCoordinateTransformation.create( t ), addInverse );
	}

	public void addTransform( NgffCoordinateTransformation<?> t ) {
		addTransform( t, true );
	}

	@SuppressWarnings( { "rawtypes", "unchecked" } )
	private void addTransform( NgffCoordinateTransformation<?> t, boolean addInverse ) {
		if( transforms.stream().anyMatch( x -> {
			if( x.getName() != null  )
				return x.getName().equals(t.getName());
			else
				return false;
		}))
		{
			return;
		}

		if ( hasSpace( t.getInput() ) && hasSpace( t.getOutput() ) )
		{
			final CoordinateSystem src = getInput( t );
			if( nameToNodes.containsKey( src ))
				nameToNodes.get( src ).edges().add( t );
			else
			{
				CoordinateSystemNode node = new CoordinateSystemNode( src );
				node.edges().add( t );
				nameToNodes.put( src.getName(), node );
			}
			transforms.add(t);
		}
		else
		{
			System.out.println( "adding despite missing space: " + t );
//			spaces.makeDefault( t.getInputAxes() )
			transforms.add( t );
		}


//		// TODO add inverse paths
		if( addInverse && t.isInvertible() )
		{
			addTransform( new InverseNgffCoordinateTransformation( t ), false );
		}
	}
	
//	public void updateTransforms()
//	{
//		getSpaces().updateTransforms( getTransforms().stream() );
//	}

	public void addCoordinateSystem( CoordinateSystem cs )
	{
		coordinateSystems.add( cs );
		nameToCoordinateSystem.put( cs.getName(), cs );
	}

//	public void add( TransformGraph g )
//	{
//		g.spaces.spaces().forEach( s -> addSpace(s));
//		g.transforms.stream().forEach( t -> addTransform(t));
//	}
	
	private CoordinateSystemNode getNode( String name )
	{
		return nameToNodes.get( name );
	}

	private CoordinateSystemNode getNode( CoordinateSystem cs )
	{
		return getNode( cs.getName() );
	}

	public Optional<TransformPath> path(final String from, final String to ) {
		return path( getCoordinateSystem( from ), getCoordinateSystem( to ) );
	}

	public Optional<TransformPath> path(final CoordinateSystem from, final CoordinateSystem to ) {

		if( from == null || to == null )
			return Optional.empty();
		else if( from.equals(to))
			return Optional.of( new TransformPath(
					new NgffIdentityTransformation("identity", from.getName(), to.getName())));

		return allPaths( from ).stream().filter( p -> getCoordinateSystem(p.getEnd()).equals(to)).findAny();
	}

	public List<TransformPath> paths(final CoordinateSystem from, final CoordinateSystem to ) {

		return allPaths( from ).stream().filter( p -> p.getEnd().equals(to)).collect(Collectors.toList());
	}

	public List<TransformPath> allPaths(final String from) {
		return allPaths(getCoordinateSystem(from));
	}

	public List<TransformPath> allPaths(final CoordinateSystem from) {

		final ArrayList<TransformPath> paths = new ArrayList<TransformPath>();
		allPathsHelper( paths, from, null );
		return paths;
	}

	private void allPathsHelper( final List< TransformPath > paths, final CoordinateSystem start, final TransformPath pathToStart )
	{
		CoordinateSystemNode node = getNode( start );

		List< NgffCoordinateTransformation<?> > edges = null;
		if ( node != null )
			edges = getNode( start ).edges();

		if ( edges == null || edges.size() == 0 )
			return;

		for ( NgffCoordinateTransformation<?> t : edges )
		{
			final CoordinateSystem end = getOutput( t );
			if ( pathToStart != null && pathToStart.hasSpace( end ) )
				continue;

			final TransformPath p;
			if ( pathToStart == null )
				p = new TransformPath( t );
			else
				p = new TransformPath( pathToStart, t );

			paths.add( p );
			allPathsHelper( paths, end, p );
		}
	}
	
	public void printSummary()
	{
		StringBuffer sb = new StringBuffer();
		sb.append( String.format( "Coordinate systems (%d) :\n", coordinateSystems.size() ));
		for( CoordinateSystem cs : coordinateSystems )
			sb.append( "\t" + cs.getName() + "\n" );

		sb.append( String.format( "Coordinate transformations (%d) :\n", transforms.size() ));
		for( NgffCoordinateTransformation< ? > ct : transforms )
			sb.append( "\t" + ct.toString() + "\n" );

		System.out.print( sb );
	}

}

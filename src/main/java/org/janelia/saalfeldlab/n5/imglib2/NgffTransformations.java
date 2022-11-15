package org.janelia.saalfeldlab.n5.imglib2;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Optional;

import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5FSReader;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.metadata.graph.TransformGraph;
import org.janelia.saalfeldlab.n5.metadata.graph.TransformPath;
import org.janelia.saalfeldlab.n5.metadata.omengff.NgffCoordinateTransformation;
import org.janelia.saalfeldlab.n5.metadata.omengff.NgffCoordinateTransformationAdapter;
import org.janelia.saalfeldlab.n5.metadata.omengff.NgffTranslationTransformation;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;

import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.basictypeaccess.array.IntArray;
import net.imglib2.realtransform.AffineGet;
import net.imglib2.realtransform.RealTransform;
import net.imglib2.realtransform.RealTransformSequence;
import net.imglib2.realtransform.TransformUtils;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.integer.IntType;
import net.imglib2.util.Intervals;
import net.imglib2.view.IntervalView;
import ome.ngff.axes.Axis;
import ome.ngff.axes.CoordinateSystem;
import ome.ngff.transformations.CoordinateTransformation;
import ome.ngff.transformations.CoordinateTransformationAdapter;

public class NgffTransformations
{

	public static void main( String[] args ) throws Exception
	{
//		final String path = "/home/john/projects/ngff/dfieldTest/dfield.n5";
		final String path = "/home/john/projects/ngff/dfieldTest/jrc18_example.n5";

		final String dataset = "/";
//		final String dataset = "coordinateTransformations";
//		final String dataset = "/dfield";

		final GsonBuilder gsonBuilder = new GsonBuilder();
		gsonBuilder.registerTypeAdapter(CoordinateTransformation.class, new CoordinateTransformationAdapter() );
		final N5FSReader n5 = new N5FSReader( path, gsonBuilder );

//		RealTransform dfieldTform = open( n5, dataset );
//		System.out.println( dfieldTform );

//		RealTransform dfieldTform = open( n5, dataset );
//		System.out.println( dfieldTform );

		TransformGraph g = openGraph( n5, dataset );
		g.printSummary();
		RealTransform fwdXfm = g.path( "jrc18F", "fcwb" ).get().totalTransform( n5, g );
		RealTransform invXfm = g.path( "fcwb", "jrc18F" ).get().totalTransform( n5, g );
		System.out.println( fwdXfm );
		System.out.println( invXfm );


//		ArrayImg< IntType, IntArray > img = ArrayImgs.ints( 2, 3, 4, 5 );
//
//		int[] p = vectorAxisLastNgff( n5, dataset );
//		System.out.println( Arrays.toString( p ));
//		System.out.println( "" );
//
//		IntervalView< IntType > imgP = N5DisplacementField.permute( img, p );	
//		System.out.println( Intervals.toString( imgP ));

		
//		try
//		{
////			AffineGet p2p = N5DisplacementField.openPixelToPhysicalNgff( n5, "transform", true );
////			System.out.println( p2p );
//
////			int[] indexes = new int[] {1, 2, 3 };
////			AffineGet sp2p = TransformUtils.subAffine( p2p, indexes );
////			System.out.println( sp2p );
//		}
//		catch ( Exception e )
//		{
//			e.printStackTrace();
//		}

	}
	
	public static TransformGraph openGraph( N5Reader n5 ) 
	{
		return openGraph( n5, "/" );
	}

	public static TransformGraph openGraph( N5Reader n5, String dataset ) 
	{
		return new TransformGraph( n5, dataset );
	}

	public static RealTransform open( N5Reader n5, String dataset ) 
	{
		// TODO error handling
		return openGraph( n5, dataset ).getTransforms().get( 0 ).getTransform( n5 );
	}

	public static RealTransform open( N5Reader n5, String dataset, String name ) 
	{
		// TODO error handling
		return openGraph( n5, dataset ).getTransform( name ).get().getTransform( n5 );
	}

	public static < T extends RealTransform> T open( N5Reader n5, String dataset, String input, String output ) 
	{
		// TODO error handling
		TransformGraph g = openGraph( n5, dataset );
		return (T)g.path( input, output ).get().totalTransform( n5, g );
	}
	
	public static RealTransform open( final String url )
	{
		// TODO
		return null;
	}
	
	public static RealTransform openJson( final String url )
	{
		final Path path = Paths.get( url );
		String string;
		try
		{
			string = new String(Files.readAllBytes(path));
		}
		catch ( IOException e )
		{
			return null;
		}

		final GsonBuilder gb = new GsonBuilder();
		gb.registerTypeAdapter(CoordinateTransformation.class, new CoordinateTransformationAdapter() );
		final Gson gson = gb.create();
		
		JsonElement elem = gson.fromJson( string, JsonElement.class );
//		System.out.println( elem );
		
//		final CoordinateTransformation ct = gson.fromJson( elem.getAsJsonArray().get( 0 ), CoordinateTransformation.class );
		final CoordinateTransformation ct = gson.fromJson( elem, CoordinateTransformation.class );
//		System.out.println( ct );
		
		NgffCoordinateTransformation< ? > nct = NgffCoordinateTransformation.create( ct );
		final RealTransform tform = nct.getTransform( null );
		System.out.println( tform );

		return tform;
	}

	/**
	 * returns null if no permutation needed
	 * 
	 * @param cs
	 * @return a permutation if needed
	 * @throws Exception
	 */
	public static final int[] vectorAxisLastNgff( CoordinateSystem cs ) throws Exception {
		
		final Axis[] axes = cs.getAxes();
		final int n = axes.length;
	
		if ( axes[ n - 1 ].getType().equals( Axis.DISPLACEMENT_TYPE ))
			return null;
		else 
		{
			int vecDim = -1;
//			for( int i = 0; i < n; i++ )
//				{
//					vecDim = i;
//					break;
//				}
//	
//			if( vecDim < 0 )
//				return null;

			final int[] permutation = new int[ n ];

			int k = 0;
			for( int i = 0; i < n; i++ )
			{
				if ( axes[ i ].getType().equals( Axis.DISPLACEMENT_TYPE ))
					vecDim = i;
				else
					permutation[i] = k++;
			}
	
			// did not find a matching axis
			if( vecDim < 0 )
				return null;

			permutation[vecDim] = n-1;
			return permutation;
		}
	}

	/**
	 * @throws Exception the exception
	 */
	public static final int[] vectorAxisLastNgff(
			final N5Reader n5, String dataset ) throws Exception {

		// TODO move to somewhere more central
		TransformGraph g = openGraph( n5, dataset );
		
		// TODO need to be smarter about which coordinate system to get
		CoordinateSystem cs = g.getCoordinateSystems().iterator().next();
		return vectorAxisLastNgff( cs );
	}


}

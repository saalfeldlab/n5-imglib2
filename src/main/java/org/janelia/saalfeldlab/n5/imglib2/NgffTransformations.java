package org.janelia.saalfeldlab.n5.imglib2;

import java.io.IOException;
import java.util.Optional;

import org.janelia.saalfeldlab.n5.N5FSReader;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.metadata.graph.TransformGraph;
import org.janelia.saalfeldlab.n5.metadata.graph.TransformPath;
import org.janelia.saalfeldlab.n5.metadata.omengff.NgffCoordinateTransformation;
import org.janelia.saalfeldlab.n5.metadata.omengff.NgffCoordinateTransformationAdapter;
import org.janelia.saalfeldlab.n5.metadata.omengff.NgffTranslationTransformation;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import net.imglib2.realtransform.AffineGet;
import net.imglib2.realtransform.RealTransform;
import net.imglib2.realtransform.RealTransformSequence;
import net.imglib2.realtransform.TransformUtils;
import ome.ngff.axes.CoordinateSystem;
import ome.ngff.transformations.CoordinateTransformation;
import ome.ngff.transformations.CoordinateTransformationAdapter;

public class NgffTransformations
{

	public static void main( String[] args ) throws IOException
	{
		final String path = "/home/john/projects/ngff/dfieldTest/dfield.n5";
		final String dataset = "/";
//		final String dataset = "coordinateTransformations";

		final GsonBuilder gsonBuilder = new GsonBuilder();
		gsonBuilder.registerTypeAdapter(CoordinateTransformation.class, new CoordinateTransformationAdapter() );
		final N5FSReader n5 = new N5FSReader( path, gsonBuilder );

		RealTransform dfieldTform = open( n5, dataset );
		System.out.println( dfieldTform );
		
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

	public static RealTransform open( N5Reader n5, String dataset, String input, String output ) 
	{
		// TODO error handling
		TransformGraph g = openGraph( n5, dataset );
		return g.path( input, output ).get().totalTransform( n5, g );
	}

}

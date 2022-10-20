package org.janelia.saalfeldlab.n5.metadata.ome.ngff;

import org.janelia.saalfeldlab.n5.metadata.omengff.NgffCoordinateTransformation;
import org.janelia.saalfeldlab.n5.metadata.omengff.NgffCoordinateTransformationAdapter;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import net.imglib2.RealPoint;
import ome.ngff.transformations.CoordinateTransformation;
import ome.ngff.transformations.CoordinateTransformationAdapter;

public class OmeNgffParseTest
{

	public static void main( String[] args )
	{
		String scaleJson = "{ \"type\": \"scale\", \"scale\": [-1,2,3]}";
		String identityJson = "{ \"type\": \"identity\"}";
		
		String useMe = identityJson;

		RealPoint p = new RealPoint( 10, 100, 1000 );
		RealPoint q = new RealPoint( 0, 0, 0 );
		
		final GsonBuilder gsonBuilder = new GsonBuilder();
//		gsonBuilder.registerTypeAdapter(CoordinateTransformation.class, new CoordinateTransformationAdapter() );
		gsonBuilder.registerTypeAdapter(NgffCoordinateTransformation.class, new NgffCoordinateTransformationAdapter() );
		final Gson gson = gsonBuilder.create();
		
//		CoordinateTransformation ct = gson.fromJson( useMe, CoordinateTransformation.class );
//		System.out.println( ct.getClass() );

		NgffCoordinateTransformation nct = gson.fromJson( useMe, NgffCoordinateTransformation.class );
		System.out.println( nct.getClass() );

		nct.getTransform().apply( p, q );
		System.out.println( p + " > " + q );

		
//		NgffCoordinateTransformation< ? > ctImglib = NgffCoordinateTransformation.create( ct );
//		System.out.println( ctImglib.getClass() );
		
//		ctImglib.getTransform().apply( p, q );
//		System.out.println( p + " > " + q );


	}

}

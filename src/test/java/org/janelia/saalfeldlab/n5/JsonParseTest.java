package org.janelia.saalfeldlab.n5;

import org.janelia.saalfeldlab.n5.imglib2.NgffTransformations;

import net.imglib2.realtransform.RealTransform;

public class JsonParseTest
{

	public static void main( String[] args )
	{
		
//		String p = "/home/john/dev/n5/n5-imglib2/src/test/resources/transforms/transformsWithAxes.json";
		String p = "/home/john/dev/n5/n5-imglib2/src/test/resources/transforms/transformList.json";

		RealTransform tform = NgffTransformations.openJson( p );


		System.out.println( "done");
	}

}

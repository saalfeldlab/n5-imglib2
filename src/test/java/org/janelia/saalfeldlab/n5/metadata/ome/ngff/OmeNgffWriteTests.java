package org.janelia.saalfeldlab.n5.metadata.ome.ngff;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.GzipCompression;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.imglib2.N5DisplacementField;

import com.google.gson.GsonBuilder;

import net.imglib2.FinalInterval;
import net.imglib2.Interval;
import net.imglib2.realtransform.AffineTransform3D;
import net.imglib2.realtransform.DisplacementFieldTransform;
import ome.ngff.axes.Axis;
import ome.ngff.axes.CoordinateSystem;
import ome.ngff.transformations.CoordinateTransformation;
import ome.ngff.transformations.CoordinateTransformationAdapter;

public class OmeNgffWriteTests
{
	public static void main( String[] args ) throws IOException
	{
		jrc18Dfield( args );
	}

	public static void jrc18Dfield( String[] args ) throws IOException
	{
		GsonBuilder gb = new GsonBuilder();
		gb.registerTypeAdapter(CoordinateTransformation.class, new CoordinateTransformationAdapter() );
		

		N5Writer n5 = new N5FSWriter("/home/john/projects/ngff/dfieldTest/dfield.n5", gb );
		int[] blockSize = new int[] { 16, 16, 16 };
		Compression compression = new GzipCompression();
		ExecutorService exec = Executors.newSingleThreadExecutor();
		
		CoordinateSystem incs = new CoordinateSystem( "in", new Axis[] {
				new Axis( "i", "space", "micrometer" ),
				new Axis( "j", "space", "micrometer" ),
				new Axis( "k", "space", "micrometer" )
		});

		CoordinateSystem outcs = new CoordinateSystem( "out", new Axis[] {
				new Axis( "x", "space", "micrometer" ),
				new Axis( "y", "space", "micrometer" ),
				new Axis( "z", "space", "micrometer" )
		});

//		N5DisplacementField.saveDisplacementFieldNgff( n5, dataset, metadataDataset,
//				incs, outcs, 
//				transform, interval, spacing, offset, 
//				blockSize, compression, exec );

	}

	public static void identity( String[] args ) throws IOException
	{
		AffineTransform3D transform = new AffineTransform3D();
		Interval interval = new FinalInterval( 16, 16, 16 );
		final double[] spacing = new double[] { 2, 2, 4 };
		final double[] offset = new double[] { -10, -10, 20 };

		String dataset = "transform";
		String metadataDataset = "/";
		
		GsonBuilder gb = new GsonBuilder();
		gb.registerTypeAdapter(CoordinateTransformation.class, new CoordinateTransformationAdapter() );

		N5Writer n5 = new N5FSWriter("/home/john/projects/ngff/dfieldTest/dfield.n5", gb );
		int[] blockSize = new int[] { 16, 16, 16 };
		Compression compression = new GzipCompression();
		ExecutorService exec = Executors.newSingleThreadExecutor();
		
		CoordinateSystem incs = new CoordinateSystem( "in", new Axis[] {
				new Axis( "i", "space", "micrometer" ),
				new Axis( "j", "space", "micrometer" ),
				new Axis( "k", "space", "micrometer" )
		});

		CoordinateSystem outcs = new CoordinateSystem( "out", new Axis[] {
				new Axis( "x", "space", "micrometer" ),
				new Axis( "y", "space", "micrometer" ),
				new Axis( "z", "space", "micrometer" )
		});

		N5DisplacementField.saveDisplacementFieldNgff( n5, dataset, metadataDataset,
				incs, outcs, 
				transform, interval, spacing, offset, 
				blockSize, compression, exec );

		N5DisplacementField.displacementFieldNgffMetadata( n5, dataset, metadataDataset, incs, outcs );


//		CoordinateSystem[] css = n5.getAttribute( metadataDataset, CoordinateSystem.KEY, CoordinateSystem[].class );
//		CoordinateTransformation[] cts = n5.getAttribute( metadataDataset, CoordinateTransformation.KEY, CoordinateTransformation[].class );
//		
//		System.out.println( css.length );
//		System.out.println( cts.length );
//		
//		String inCsName = incs.getName();
//		String outCsName = outcs.getName();
//		CoordinateTransformation ctParsed = null;
//		for ( CoordinateTransformation ct : cts )
//		{
//			if( ct.getInput().equals( inCsName ) && ct.getOutput().equals( outCsName ))
//			{
//				ctParsed = ct;
//				break;
//			}
//		}
//
//		System.out.println( ctParsed.getType() );
		
		//N5DisplacementField.openDisplacementFieldNgff( n5, metadataDataset, dataset, incs.getName(), exec );
//		DisplacementFieldTransform dfield = N5DisplacementField.openDisplacementFieldNgff( n5, dataset, metadataDataset, exec );
//		System.out.println( dfield );


		n5.close();
		System.exit( 0 );
	}

}

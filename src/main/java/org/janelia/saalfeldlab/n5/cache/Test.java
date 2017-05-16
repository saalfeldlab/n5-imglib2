package org.janelia.saalfeldlab.n5.cache;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.Callable;

import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5;

import bdv.util.BdvFunctions;
import bdv.util.BdvOptions;
import bdv.util.BdvStackSource;
import bdv.viewer.DisplayMode;
import net.imglib2.RealRandomAccessible;
import net.imglib2.cache.img.DiskCellCache;
import net.imglib2.cache.img.PrimitiveType;
import net.imglib2.cache.queue.BlockingFetchQueues;
import net.imglib2.cache.queue.FetcherThreads;
import net.imglib2.cache.util.IntervalKeyLoaderAsLongKeyLoader;
import net.imglib2.img.Img;
import net.imglib2.img.basictypeaccess.volatiles.array.VolatileByteArray;
import net.imglib2.img.cell.CellGrid;
import net.imglib2.interpolation.randomaccess.NearestNeighborInterpolatorFactory;
import net.imglib2.realtransform.AffineTransform3D;
import net.imglib2.realtransform.InverseRealTransform;
import net.imglib2.realtransform.RealTransformRealRandomAccessible;
import net.imglib2.realtransform.RealViews;
import net.imglib2.type.numeric.integer.UnsignedByteType;
import net.imglib2.type.volatiles.VolatileUnsignedByteType;
import net.imglib2.util.Pair;
import net.imglib2.view.Views;

public class Test
{

	public static void main( final String[] args ) throws IOException
	{
		// davi
//		final String n5path = "/data/hanslovskyp/n5-tests/davi-toy-set";
//		final String dataset = "excerpt";

		// tomoko
		final String n5path = "/data/hanslovskyp/tomoko/n5-test";
		final String dataset = "z=0,512-raw";
		final N5 n5 = new N5( n5path );
		final DatasetAttributes attr = n5.getDatasetAttributes( dataset );
		final long[] dim = attr.getDimensions();
		final int[] cellSize = attr.getBlockSize();

		final CellGrid grid = new CellGrid( dim, cellSize );

		System.out.println( attr.getNumDimensions() + " " + attr.getDataType() + " " + attr.getCompressionType() + " " + Arrays.toString( attr.getDimensions() ) + " " + Arrays.toString( attr.getBlockSize() ) );

		final int numProc = Runtime.getRuntime().availableProcessors();
		final int maxNumLevels = 1;
		final int numFetcherThreads = numProc - 1;
		final BlockingFetchQueues< Callable< ? > > queue = new BlockingFetchQueues<>( maxNumLevels );
		new FetcherThreads( queue, numFetcherThreads );

		final N5Loader< VolatileByteArray > loader = new N5Loader<>( n5, dataset, cellSize, N5Loader.defaultArrayAccessGenerator( true ) );
		final IntervalKeyLoaderAsLongKeyLoader< VolatileByteArray > longKeyLoader = new IntervalKeyLoaderAsLongKeyLoader<>( grid, loader );

		final Pair< Img< UnsignedByteType >, Img< VolatileUnsignedByteType > > imgs =
				CacheUtil.createImgAndVolatileImgFromCacheLoader( grid, queue, longKeyLoader, new UnsignedByteType(), new VolatileUnsignedByteType(), PrimitiveType.BYTE, DiskCellCache.createTempDirectory( "blocks", true ) );


		final AffineTransform3D tf = new AffineTransform3D();
		tf.setTranslation( 1000, 1000, 0 );
		final RealRandomAccessible< UnsignedByteType > extendedAndInterpolated = Views.interpolate( Views.extendValue( imgs.getA(), new UnsignedByteType( 255 ) ), new NearestNeighborInterpolatorFactory<>() );
		final RealTransformRealRandomAccessible< UnsignedByteType, InverseRealTransform > transformed = RealViews.transformReal( extendedAndInterpolated, tf );

		final IntervalKeyLoaderAsLongKeyLoader< VolatileByteArray > otherLoader = new IntervalKeyLoaderAsLongKeyLoader<>( grid, new TransformedAccessibleLoader<>( Views.raster( transformed ), new UnsignedByteType() ) );

		final Pair< Img< UnsignedByteType >, Img< VolatileUnsignedByteType > > imgs2 =
				CacheUtil.createImgAndVolatileImgFromCacheLoader( grid, queue, otherLoader, new UnsignedByteType(), new VolatileUnsignedByteType(), PrimitiveType.BYTE, DiskCellCache.createTempDirectory( "translated", true ) );

		final BdvStackSource< VolatileUnsignedByteType > bdv = BdvFunctions.show( imgs.getB(), "volatile!" );
		BdvFunctions.show( imgs2.getB(), "translated!", BdvOptions.options().addTo( bdv ) );

		bdv.getBdvHandle().getSetupAssignments().getMinMaxGroups().get( 0 ).setRange( 0, 255 );
		bdv.getBdvHandle().getSetupAssignments().getMinMaxGroups().get( 1 ).setRange( 0, 255 );
		bdv.getBdvHandle().getViewerPanel().setDisplayMode( DisplayMode.SINGLE );

	}

}

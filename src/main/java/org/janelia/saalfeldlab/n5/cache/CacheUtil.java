package org.janelia.saalfeldlab.n5.cache;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.Callable;

import bdv.img.cache.CreateInvalidVolatileCell;
import bdv.img.cache.VolatileCachedCellImg;
import net.imglib2.cache.Cache;
import net.imglib2.cache.CacheLoader;
import net.imglib2.cache.IoSync;
import net.imglib2.cache.img.AccessFlags;
import net.imglib2.cache.img.AccessIo;
import net.imglib2.cache.img.DiskCellCache;
import net.imglib2.cache.img.PrimitiveType;
import net.imglib2.cache.queue.BlockingFetchQueues;
import net.imglib2.cache.ref.GuardedStrongRefLoaderRemoverCache;
import net.imglib2.cache.ref.WeakRefVolatileCache;
import net.imglib2.cache.volatiles.CacheHints;
import net.imglib2.cache.volatiles.CreateInvalid;
import net.imglib2.cache.volatiles.LoadingStrategy;
import net.imglib2.cache.volatiles.VolatileCache;
import net.imglib2.img.Img;
import net.imglib2.img.basictypeaccess.volatiles.VolatileArrayDataAccess;
import net.imglib2.img.cell.Cell;
import net.imglib2.img.cell.CellGrid;
import net.imglib2.img.cell.LazyCellImg;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.volatiles.AbstractVolatileNativeRealType;
import net.imglib2.util.Pair;
import net.imglib2.util.ValuePair;

public class CacheUtil
{

	public static < T extends RealType< T > & NativeType< T >, VT extends AbstractVolatileNativeRealType< T, VT >, A extends VolatileArrayDataAccess< A > >
	Pair< Img< T >, Img< VT > >
	createImgAndVolatileImgFromCacheLoader(
			final CellGrid grid,
			final BlockingFetchQueues< Callable< ? > > queue,
			final CacheLoader< Long, Cell< A > > loader,
			final T type,
			final VT vtype,
			final PrimitiveType primitiveType,
			final Path blockcache )
					throws IOException
	{

//		final Path blockcache = DiskCellCache.createTempDirectory( name, deleteOnExit );

		final DiskCellCache< A > diskcache = new DiskCellCache<>(
				blockcache,
				grid,
				loader,
				AccessIo.get( primitiveType, AccessFlags.VOLATILE ),
				type.getEntitiesPerPixel() );
		final IoSync< Long, Cell< A > > iosync = new IoSync<>( diskcache );
		final Cache< Long, Cell< A > > cache = new GuardedStrongRefLoaderRemoverCache< Long, Cell< A > >( 1000 )
				.withRemover( iosync )
				.withLoader( iosync );
		final LazyCellImg< T, A > http = new LazyCellImg<>( grid, type, cache.unchecked()::get );

		final CreateInvalid< Long, Cell< A > > createInvalid = CreateInvalidVolatileCell.get( grid, type );
		final VolatileCache< Long, Cell< A > > volatileCache = new WeakRefVolatileCache<>( cache, queue, createInvalid );

		final CacheHints hints = new CacheHints( LoadingStrategy.VOLATILE, 0, false );
		final VolatileCachedCellImg< VT, A > vhttp = new VolatileCachedCellImg<>( grid, vtype, hints, volatileCache.unchecked()::get );

		return new ValuePair<>( http, vhttp );
	}

}

package org.janelia.saalfeldlab.n5.metadata.omengff;

import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.imglib2.N5DisplacementField;

import net.imglib2.RandomAccessible;
import net.imglib2.RealRandomAccessible;
import net.imglib2.interpolation.InterpolatorFactory;
import net.imglib2.realtransform.DisplacementFieldTransform;
import net.imglib2.realtransform.TransformUtils;
import net.imglib2.type.numeric.real.DoubleType;
import net.imglib2.view.composite.RealComposite;
import ome.ngff.transformations.DisplacementsTransformation;

public class NgffDisplacementsTransformation extends DisplacementsTransformation
	implements NgffCoordinateTransformation< DisplacementFieldTransform >
{

	public NgffDisplacementsTransformation( final String name, final String input, final String output, final String path, final String interpolation )
	{
		super( name, input, output, path, interpolation );
	}

	public NgffDisplacementsTransformation( final String input, final String output, final String path, final String interpolation )
	{
		this( null, input, output, path, interpolation );
	}
	
	public NgffDisplacementsTransformation( final String path, final String interpolation )
	{
		this( null, null, null, path, interpolation );
	}

	public NgffDisplacementsTransformation( DisplacementsTransformation other )
	{
		super( other );
	}

	@Override
	public DisplacementFieldTransform getTransform()
	{
		return null;
	}

	@Override
	public DisplacementFieldTransform getTransform( final N5Reader n5 )
	{
		InterpolatorFactory< RealComposite<DoubleType>, RandomAccessible<RealComposite<DoubleType>>> interp = TransformUtils.interpolator( getInterpolation() );
		try
		{
			RealRandomAccessible< RealComposite< DoubleType > > dfield = N5DisplacementField.openCalibratedFieldNgff( 
					n5, getPath(), interp, N5DisplacementField.EXTEND_BORDER, new DoubleType() );
			return new DisplacementFieldTransform( dfield );
		}
		catch ( Exception e )
		{
			e.printStackTrace();
		}
		return null;
	}

}

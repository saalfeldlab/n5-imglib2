package org.janelia.saalfeldlab.n5.metadata.omengff;

import org.janelia.saalfeldlab.n5.N5Reader;

import net.imglib2.realtransform.Translation;
import net.imglib2.realtransform.Translation2D;
import net.imglib2.realtransform.Translation3D;
import net.imglib2.realtransform.TranslationGet;
import ome.ngff.transformations.TranslationTransformation;

public class NgffTranslationTransformation extends TranslationTransformation
	implements NgffInvertibleCoordinateTransformation< TranslationGet >
{
	public NgffTranslationTransformation( final double[] translation )
	{
		super( translation );
	}

	public NgffTranslationTransformation( String input, String output, double[] translation )
	{
		super( input, output, translation );
	}

	public NgffTranslationTransformation( TranslationTransformation other )
	{
		super( other );
	}

	@Override
	public TranslationGet getTransform()
	{
		int nd = getTranslation().length;
		if( nd == 2 )
			return new Translation2D( getTranslation() );
		else if( nd == 3 )
			return new Translation3D( getTranslation() );
		else
			return new Translation( getTranslation() );
	}

	@Override
	public TranslationGet getTransform( N5Reader n5 )
	{
		// TODO fix
		return getTransform();
	}

}

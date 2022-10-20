package org.janelia.saalfeldlab.n5.metadata.omengff;

import net.imglib2.realtransform.Translation;
import net.imglib2.realtransform.Translation2D;
import net.imglib2.realtransform.Translation3D;
import net.imglib2.realtransform.TranslationGet;
import ome.ngff.transformations.TranslationTransformation;

public class NgffTranslationTransformation extends TranslationTransformation
	implements NgffCoordinateTransformation< TranslationGet >
{

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

}

package org.janelia.saalfeldlab.n5.metadata.ome.v03;

import org.janelia.saalfeldlab.n5.metadata.N5Metadata;

public class OmeNgffMetadataV03 implements N5Metadata
{
	public final String path;

	public final OmeNgffMultiScaleMetadataV03[] multiscales;

	public OmeNgffMetadataV03( final String path,
			final OmeNgffMultiScaleMetadataV03[] multiscales )
	{
		this.path = path;
		this.multiscales = multiscales;
	}

	@Override
	public String getPath()
	{
		return path;
	}

}

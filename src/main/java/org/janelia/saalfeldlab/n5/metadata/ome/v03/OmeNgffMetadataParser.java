package org.janelia.saalfeldlab.n5.metadata.ome.v03;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5TreeNode;
import org.janelia.saalfeldlab.n5.metadata.N5MetadataParser;
import org.janelia.saalfeldlab.n5.metadata.N5SingleScaleMetadata;

public class OmeNgffMetadataParser implements N5MetadataParser< OmeNgffMetadata >
{

	@Override
	public Optional< OmeNgffMetadata > parseMetadata( N5Reader n5, N5TreeNode node )
	{

		final Map< String, N5TreeNode > scaleLevelNodes = new HashMap<>();
		for ( final N5TreeNode childNode : node.childrenList() )
		{
			if ( childNode.isDataset() && childNode.getMetadata() != null )
			{
				scaleLevelNodes.put( childNode.getNodeName(), childNode );
			}
		}

		OmeNgffMultiScaleMetadata[] multiscales;
		try
		{
			multiscales = n5.getAttribute( node.getPath(), "multiscales",
					OmeNgffMultiScaleMetadata[].class );
		} catch ( IOException e )
		{
			return Optional.empty();
		}

		if ( multiscales == null || multiscales.length == 0 )
		{
			return Optional.empty();
		}

		for ( OmeNgffMultiScaleMetadata ms : multiscales )
		{
			ms.path = node.getPath();
			DatasetAttributes[] attrs = new DatasetAttributes[ ms.getPaths().length ];
			N5SingleScaleMetadata[] childrenMeta = new N5SingleScaleMetadata[ ms
					.getPaths().length ];

			int i = 0;
			for ( String childPath : ms.getPaths() )
			{
				N5SingleScaleMetadata meta = (N5SingleScaleMetadata) scaleLevelNodes
						.get( childPath ).getMetadata();
				childrenMeta[ i ] = meta;
				System.out.println( childPath );
				attrs[ i++ ] = meta.getAttributes();
			}
			ms.childrenMetadata = childrenMeta;
			ms.childrenAttributes = attrs;
		}
		return Optional.of( new OmeNgffMetadata( node.getPath(), multiscales ) );
	}

}

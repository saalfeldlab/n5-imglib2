package org.janelia.saalfeldlab.n5.container;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DatasetAttributes;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
/**
 * 
 * @author John Bogovic
 */
public class ContainerDataNode extends ContainerMetadataNode {

	protected HashMap<long[], JsonElement> data;

	public ContainerDataNode( String path ) {
		super(path);
		data = new HashMap<long[], JsonElement>();
	}

	public ContainerDataNode() {
		this("");
	}

	public ContainerDataNode(
			final String path,
			final HashMap<String, JsonElement> attributes,
			final HashMap<long[], JsonElement> data,
			final Map<String, ContainerMetadataNode> children, final Gson gson) {
		super( path, attributes, children, gson );
		this.data = data;
	}

	public ContainerDataNode( ContainerDataNode other) {
		super(other);
		data = other.data;
	}

	public HashMap<long[], JsonElement> getData() {
		return data;
	}

	@Override
	public <T> void writeBlock(String pathName, DatasetAttributes datasetAttributes, DataBlock<T> dataBlock)
			throws IOException {
	}

	@Override
	public boolean deleteBlock(String pathName, long... gridPosition) throws IOException {
		return false;
	}

	@Override
	public DataBlock<?> readBlock(String pathName, DatasetAttributes datasetAttributes, long... gridPosition)
			throws IOException {
		return null;
	}

}

package org.janelia.saalfeldlab.n5.translation;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

import org.janelia.saalfeldlab.n5.AbstractGsonReader;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.container.ContainerMetadataNode;

import com.google.gson.Gson;
import com.google.gson.JsonElement;

public class TranslatedN5Reader extends AbstractGsonReader {
	
	private final N5Reader n5;

	protected final InvertibleTreeTranslation translation;

	public TranslatedN5Reader( final N5Reader n5Base,
			final Gson gson,
			final String fwdTranslation,
			final String invTranslation ) {
		this.n5 = n5Base;
		ContainerMetadataNode root = ContainerMetadataNode.build(n5Base, gson);
		root.addPathsRecursive();
		translation = new InvertibleTreeTranslation(root, gson, fwdTranslation, invTranslation);
	}

	public TranslatedN5Reader( final N5Reader n5Base,
			final Gson gson,
			final String fwdTranslation ) {
		this.n5 = n5Base;
		ContainerMetadataNode root = ContainerMetadataNode.build(n5Base, gson);
		root.addPathsRecursive();
		translation = new InvertibleTreeTranslation(root, gson, fwdTranslation, "." );
	}

	public InvertibleTreeTranslation getTranslation() {
		return translation;
	}

	public TranslatedN5Reader( final AbstractGsonReader n5Base,
			final String fwdTranslation,
			final String invTranslation ) {
		this( n5Base, n5Base.getGson(), fwdTranslation, invTranslation );
	}

	public TranslatedN5Reader( final AbstractGsonReader n5Base,
			final String fwdTranslation ) {
		this( n5Base, n5Base.getGson(), fwdTranslation );
	}

	@Override
	public <T> T getAttribute(String pathName, String key, Class<T> clazz) throws IOException {
		return translation.getTranslated().getAttribute(pathName, key, clazz);
	}

	@Override
	public <T> T getAttribute(String pathName, String key, Type type) throws IOException {
		return translation.getTranslated().getAttribute(pathName, key, type);
	}
	
	/**
	 * Returns the path in the original container given the path in the translated container.
	 * 
	 * @param pathName the path in the translated container
	 * @return the path in the original container
	 */
	public String originalPath( String pathName )
	{
		return translation.getTranslated().getChild( pathName ).map( ContainerMetadataNode::getDataPath )
			.orElseGet( () -> {
				ContainerMetadataNode pathNode = new ContainerMetadataNode();
				pathNode.createGroup(pathName);
				pathNode.addPathsRecursive();

				ContainerMetadataNode translatedPathNode = translation.getInverseTranslationFunction().apply(pathNode);
				translatedPathNode.addPathsRecursive();

				return translatedPathNode.flattenLeaves().findFirst().get().getPath();
			});
	}

	@Override
	public DataBlock<?> readBlock(String pathName, DatasetAttributes datasetAttributes, long... gridPosition)
			throws IOException {

		return n5.readBlock( originalPath( pathName ), datasetAttributes, gridPosition);
	}

	@Override
	public boolean exists(String pathName) {
		return translation.getTranslated().exists(pathName);
	}

	@Override
	public String[] list(String pathName) throws IOException {
		return translation.getTranslated().list(pathName);
	}

	@Override
	public Map<String, Class<?>> listAttributes(String pathName) throws IOException {
		return translation.getTranslated().listAttributes(pathName);
	}

	@Override
	public HashMap<String, JsonElement> getAttributes(String pathName) throws IOException {
		return translation.getTranslated().getNode(pathName)
				.map( ContainerMetadataNode::getAttributes )
				.orElse( new HashMap<>());
	}

}

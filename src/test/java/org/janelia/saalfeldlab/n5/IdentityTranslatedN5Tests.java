package org.janelia.saalfeldlab.n5;

import java.io.IOException;

import org.janelia.saalfeldlab.n5.translation.TranslatedN5Writer;

public class IdentityTranslatedN5Tests extends AbstractN5Test {
	
	static private String testDirPath = System.getProperty("user.home") + "/tmp/idTranslatedTest.n5";

	protected N5Writer createN5Writer() throws IOException {
		final N5FSWriter n5Base = new N5FSWriter( testDirPath );
		final TranslatedN5Writer n5 = new TranslatedN5Writer(n5Base, n5Base.getGson(), ".", "." );	
		return n5;
	}
	
}

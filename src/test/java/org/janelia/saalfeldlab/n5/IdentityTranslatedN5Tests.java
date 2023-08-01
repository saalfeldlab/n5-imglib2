package org.janelia.saalfeldlab.n5;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;

import org.janelia.saalfeldlab.n5.translation.TranslatedN5Writer;
import org.junit.Assert;
import org.junit.Test;

import com.google.gson.reflect.TypeToken;

public class IdentityTranslatedN5Tests extends AbstractN5Test {
	
	private static String testDirPath = createTestDirPath("n5-imglib2-test");

	private static String createTestDirPath(String dirName) {
		try {
			return Files.createTempDirectory(dirName).toString();
		} catch (IOException exc) {
			return System.getProperty("user.home") + "/tmp/" + dirName;
		}
	}
	protected N5Writer createN5Writer() throws IOException {
		final N5FSWriter n5Base = new N5FSWriter( testDirPath + "/idTranslatedTest.n5" );
		final TranslatedN5Writer n5 = new TranslatedN5Writer(n5Base, n5Base.getGson(), ".", "." );	
		return n5;
	}
	
	@Override
	@Test
	public void testAttributes() {

		try {
			n5.createGroup(groupName);

			n5.setAttribute(groupName, "key1", "value1");
			// path
			Assert.assertEquals(2, n5.listAttributes(groupName).size());

			/* class interface */
			Assert.assertEquals("value1", n5.getAttribute(groupName, "key1", String.class));
			/* type interface */
			Assert.assertEquals("value1", n5.getAttribute(groupName, "key1", new TypeToken<String>(){}.getType()));

			final Map<String, String> newAttributes = new HashMap<>();
			newAttributes.put("key2", "value2");
			newAttributes.put("key3", "value3");
			n5.setAttributes(groupName, newAttributes);
			Assert.assertEquals(4, n5.listAttributes(groupName).size());
			/* class interface */
			Assert.assertEquals("value1", n5.getAttribute(groupName, "key1", String.class));
			Assert.assertEquals("value2", n5.getAttribute(groupName, "key2", String.class));
			Assert.assertEquals("value3", n5.getAttribute(groupName, "key3", String.class));
			/* type interface */
			Assert.assertEquals("value1", n5.getAttribute(groupName, "key1", new TypeToken<String>(){}.getType()));
			Assert.assertEquals("value2", n5.getAttribute(groupName, "key2", new TypeToken<String>(){}.getType()));
			Assert.assertEquals("value3", n5.getAttribute(groupName, "key3", new TypeToken<String>(){}.getType()));

			// test the case where the resulting file becomes shorter
			n5.setAttribute(groupName, "key1", new Integer(1));
			n5.setAttribute(groupName, "key2", new Integer(2));
			Assert.assertEquals(4, n5.listAttributes(groupName).size());
			/* class interface */
			Assert.assertEquals(new Integer(1), n5.getAttribute(groupName, "key1", Integer.class));
			Assert.assertEquals(new Integer(2), n5.getAttribute(groupName, "key2", Integer.class));
			Assert.assertEquals("value3", n5.getAttribute(groupName, "key3", String.class));
			/* type interface */
			Assert.assertEquals(new Integer(1), n5.getAttribute(groupName, "key1", new TypeToken<Integer>(){}.getType()));
			Assert.assertEquals(new Integer(2), n5.getAttribute(groupName, "key2", new TypeToken<Integer>(){}.getType()));
			Assert.assertEquals("value3", n5.getAttribute(groupName, "key3", new TypeToken<String>(){}.getType()));

		} catch (final IOException e) {
			fail(e.getMessage());
		}
	}

}

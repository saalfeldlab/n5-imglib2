package org.janelia.saalfeldlab.n5;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class N5TreeNodeTest {

	@Test
	public void testAddingChildren()
	{
		N5TreeNode root = new N5TreeNode("");
		root.addPath("a/b/c");
		assertTrue( root.getDescendant("a/b/c").isPresent() );

		root.add(new N5TreeNode("ant"));
		assertTrue( root.getDescendant("ant").isPresent() );

		root.addPath("ant/bat");
		assertTrue( root.getDescendant("ant/bat").isPresent() );
	}

}

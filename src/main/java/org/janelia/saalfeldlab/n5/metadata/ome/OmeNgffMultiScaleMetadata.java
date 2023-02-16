/**
 * Copyright (c) 2018--2020, Saalfeld lab
 * All rights reserved.
 * <p>
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * <p>
 * 1. Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 * <p>
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package org.janelia.saalfeldlab.n5.metadata.ome;

import java.util.Arrays;
import java.util.stream.DoubleStream;

import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.metadata.MultiscaleMetadata;
import org.janelia.saalfeldlab.n5.metadata.N5SingleScaleMetadata;
import org.janelia.saalfeldlab.n5.metadata.SpatialMetadataGroup;

import com.google.gson.JsonObject;

import net.imglib2.realtransform.AffineTransform3D;

/**
 * The multiscales metadata for the OME NGFF specification.
 * <p>
 * See <a href="https://ngff.openmicroscopy.org/0.3/#multiscale-md">https://ngff.openmicroscopy.org/0.3/#multiscale-md</a>
 * 
 * @author John Bogovic
 */
public class OmeNgffMultiScaleMetadata extends MultiscaleMetadata<N5SingleScaleMetadata> {

	public final String name;
	public final String type;
	public final String version;
	public final String[] axes;
	public final OmeNgffDataset[] datasets;
	public final OmeNgffDownsamplingMetadata metadata;

	public transient String path;
	public transient N5SingleScaleMetadata[] childrenMetadata;
	public transient DatasetAttributes[] childrenAttributes;

	public OmeNgffMultiScaleMetadata( 
			final String path,
			final String name, final String type, final String version,
			final String[] axes, final OmeNgffDataset[] datasets, 
			final DatasetAttributes[] childrenAttributes,
			final OmeNgffDownsamplingMetadata metadata)
	{
		super( path, buildMetadata( path, datasets, childrenAttributes, metadata ));
		this.name = name;
		this.type = type;
		this.version = version;
		this.axes = axes;
		this.datasets = datasets;
		this.childrenAttributes = childrenAttributes;
		this.metadata = metadata;
	}

	private static N5SingleScaleMetadata[] buildMetadata( 
			final String path,
			final OmeNgffDataset[] datasets,
			final DatasetAttributes[] childrenAttributes,
			final OmeNgffDownsamplingMetadata metadata )
	{
		final int N = datasets.length;
		final AffineTransform3D id = new AffineTransform3D();
		final double[] pixelRes = DoubleStream.of(1).limit(N).toArray();
		final double[] offset = DoubleStream.of(0).limit(N).toArray();

		final double[] factors;
		if( metadata.scale == null)
			factors = DoubleStream.of(1).limit(N).toArray();
		else
			factors = metadata.scale;

		N5SingleScaleMetadata[] childrenMetadata = new N5SingleScaleMetadata[ N ];
		for( int i = 0; i < N; i++ ) {
			childrenMetadata[i] = new N5SingleScaleMetadata(path, id, factors, pixelRes, offset,
					"pixel", childrenAttributes[i]);
		}
		return childrenMetadata;
	}

	@Override
	public String[] getPaths() {
		return Arrays.stream( datasets ).map( x -> { 
			if( path.isEmpty() )
				return x.path;
			else if ( path .endsWith("/"))
				return path + x.path;
			else 
				return path + "/" + x.path;
		}).toArray( String[]::new );
	}

	@Override
	public N5SingleScaleMetadata[] getChildrenMetadata() {
		return childrenMetadata;
	}

	@Override
	public String getPath() {
		return path;
	}

	@Override
	public String[] units() {
		return Arrays.stream( datasets ).map( x -> "pixel").toArray( String[]::new );
	}

	public static class OmeNgffDataset {
		public String path;
	}

	public static class OmeNgffDownsamplingMetadata {
		public int order;
		public boolean preserve_range;
		public double[] scale;
		public String method;
		public String version;
		public String args;
		public JsonObject kwargs;
	}
}

package org.janelia.saalfeldlab.n5.metadata.omengff;

import org.janelia.saalfeldlab.n5.N5Reader;

import net.imglib2.realtransform.InvertibleRealTransform;
import net.imglib2.realtransform.RealTransform;

public class InverseNgffCoordinateTransformation<T extends InvertibleRealTransform> implements NgffInvertibleCoordinateTransformation< T >
{
	protected NgffCoordinateTransformation<T> ct; 

	public InverseNgffCoordinateTransformation(NgffCoordinateTransformation<T> ct)
	{
		this.ct = ct;
	}

	@Override
	public T getTransform( N5Reader n5 )
	{
		return ( T ) ct.getTransform( n5 ).inverse();
	}

	@Override
	public RealTransform getTransform()
	{
		return ((InvertibleRealTransform)ct.getTransform()).inverse();
	}

	@Override
	public String getName()
	{
		return ct.getName() + "(inverse)";
	}

	@Override
	public String getType()
	{
		return ct.getType();
	}

	@Override
	public String getInput()
	{
		// input and output switched
		return ct.getOutput();
	}

	@Override
	public String getOutput()
	{
		// input and output switched
		return ct.getInput();
	}

	@Override
	public String[] getInputAxes()
	{
		return ct.getOutputAxes(); 
	}

	@Override
	public String[] getOutputAxes()
	{
		return ct.getInputAxes();
	}

}

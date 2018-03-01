package org.janelia.saalfeldlab.n5.imglib2;

import java.util.function.BiConsumer;

import net.imglib2.img.Img;
import net.imglib2.type.Type;

public class ExceptionHandlers
{

	public static < E extends Exception, T > BiConsumer< E, Img< T > > asRuntimeException()
	{
		return new AsRuntimeException<>();
	}

	public static < E extends Exception, T extends Type< T > > BiConsumer< E, Img< T > > fillWithDefaultValue( T defaultValue )
	{
		return new FillWithDefaultValue<>( defaultValue );
	}

	public static class AsRuntimeException< E extends Exception, T > implements BiConsumer< E, Img< T > >
	{

		@Override
		public void accept( E exception, Img< T > img )
		{
			throw new RuntimeException( exception );
		}

	}

	public static class FillWithDefaultValue< E extends Exception, T extends Type< T > > implements BiConsumer< E, Img< T > >
	{

		private final T defaultValue;

		public FillWithDefaultValue( T defaultValue )
		{
			super();
			this.defaultValue = defaultValue;
		}

		@Override
		public void accept( E exception, Img< T > img )
		{
			img.forEach( i -> i.set( defaultValue ) );
		}

	}

}

package org.apache.spark.util.func;

import java.util.Iterator;
import org.apache.spark.api.java.function.Function;

/**
 * 
 * @author bluejoe2008@gmail.com, http://github.com/bluejoe2008
 *
 */
public abstract class IteratorUtils
{

	public static <X> Iterable<X> createIterable(final Iterator<X> src)
	{
		return new Iterable<X>()
		{
			public Iterator<X> iterator()
			{
				return src;
			}
		};
	}

	public static <X, Y> Iterable<Y> map(final Iterable<X> src,
			final Function<X, Y> func)
	{
		return createIterable(map(src.iterator(), func));
	}

	public static <X, Y> Iterator<Y> map(final Iterator<X> src,
			final Function<X, Y> func)
	{
		return new Iterator<Y>()
		{

			public boolean hasNext()
			{
				return src.hasNext();
			}

			public Y next()
			{
				try
				{
					return func.call(src.next());
				}
				catch (Exception e)
				{
					throw new RuntimeException(e);
				}
			}

			public void remove()
			{
				src.remove();
			}
		};
	}
}

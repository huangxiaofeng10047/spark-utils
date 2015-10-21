package org.apache.spark.util.func;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

public abstract class MapFunctionUtils
{
	public static <X, Y> MapFunctionWrapper wrap(Function<X, Y> func)
	{
		return new MapFunctionWrapper(func);
	}

	public static <X, Y> FlatMapFunctionWrapper wrap(FlatMapFunction<X, Y> func)
	{
		return new FlatMapFunctionWrapper(func);
	}

	public static <X, Y, Z> Function<X, Z> concat(final Function<X, Y> f1,
			final Function<Y, Z> f2)
	{
		return new Function<X, Z>()
		{
			public Z call(X x) throws Exception
			{
				return f2.call(f1.call(x));
			}
		};
	}

	public static <X, Y, Z> FlatMapFunction<X, Z> concat(
			final FlatMapFunction<X, Y> f1, final Function<Y, Z> f2)
	{
		return new FlatMapFunction<X, Z>()
		{
			public Iterable<Z> call(final X x) throws Exception
			{
				return IteratorUtils.map(f1.call(x), f2);
			}
		};
	}

	public static <X, Y, Z> FlatMapFunction<X, Z> concat(
			final Function<X, Y> f1, final FlatMapFunction<Y, Z> f2)
	{
		return new FlatMapFunction<X, Z>()
		{
			public Iterable<Z> call(final X x) throws Exception
			{
				return f2.call(f1.call(x));
			}
		};
	}
}

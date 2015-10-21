package org.apache.spark.util.func;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

public class MapFunctionWrapper<X, Y>
{
	private Function<X, Y> _func;

	public Function<X, Y> unwrap()
	{
		return _func;
	}

	public MapFunctionWrapper(Function<X, Y> func)
	{
		_func = func;
	}

	public <Z> MapFunctionWrapper<X, Z> concat(Function<Y, Z> func)
	{
		return new MapFunctionWrapper<X, Z>(
				MapFunctionUtils.concat(_func, func));
	}

	public <Z> FlatMapFunctionWrapper<X, Z> concat(FlatMapFunction<Y, Z> func)
	{
		return new FlatMapFunctionWrapper<X, Z>(MapFunctionUtils.concat(_func,
				func));
	}
}

package org.apache.spark.util.func;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

public class FlatMapFunctionWrapper<X,Y>
{
	private FlatMapFunction<X, Y> _func;

	public FlatMapFunction<X, Y> unwrap()
	{
		return _func;
	}

	public FlatMapFunctionWrapper(FlatMapFunction<X,Y> func)
	{
		_func=func;
	}
	
	public <Z> FlatMapFunctionWrapper<X,Z> concat(Function<Y,Z> func)
	{
		return new FlatMapFunctionWrapper<X,Z>(MapFunctionUtils.concat(_func, func));
	}
}

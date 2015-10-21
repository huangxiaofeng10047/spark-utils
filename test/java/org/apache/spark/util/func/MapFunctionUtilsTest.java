package org.apache.spark.util.func;

import java.util.Arrays;
import java.util.Iterator;

import junit.framework.Assert;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.junit.Test;

public class MapFunctionUtilsTest
{
	Function<Integer, Integer> _funcMap = new Function<Integer, Integer>()
	{
		public Integer call(Integer v1) throws Exception
		{
			return v1 * 2;
		}
	};

	FlatMapFunction<Integer, Integer> _funcFlatMap = new FlatMapFunction<Integer, Integer>()
	{
		public Iterable<Integer> call(Integer v1) throws Exception
		{
			return Arrays.asList(new Integer[] { v1, v1 + 1, v1 + 2 });
		}
	};

	private void assertEquals(int[] expected, Iterable<Integer> list)
	{
		Iterator<Integer> it = list.iterator();
		for (int i : expected)
		{
			Assert.assertTrue(it.hasNext());
			Assert.assertEquals(i, it.next().intValue());
		}

		Assert.assertFalse(it.hasNext());
	}

	@Test
	public void test() throws Exception
	{
		Assert.assertEquals(2, MapFunctionUtils.wrap(_funcMap).unwrap().call(1));
		Assert.assertEquals(4, MapFunctionUtils.wrap(_funcMap).concat(_funcMap)
				.unwrap().call(1));

		assertEquals(new int[] { 2, 3, 4 }, MapFunctionUtils.wrap(_funcMap)
				.concat(_funcFlatMap).unwrap().call(1));
		assertEquals(new int[] { 2, 4, 6 }, MapFunctionUtils.wrap(_funcFlatMap)
				.concat(_funcMap).unwrap().call(1));
	}

}

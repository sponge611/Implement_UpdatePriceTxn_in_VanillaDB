package org.vanilladb.bench.benchmarks.as2.rte;

import java.util.LinkedList;

import org.vanilladb.bench.benchmarks.as2.As2BenchConstants;
import org.vanilladb.bench.benchmarks.as2.As2BenchTxnType;
import org.vanilladb.bench.rte.TxParamGenerator;
import org.vanilladb.bench.util.RandomValueGenerator;

//Implement assignment2 by Tsai-Che-Wei

public class As2UpdateItemParamGen implements TxParamGenerator<As2BenchTxnType> {
	
	private static final int UPDATE_COUNT = 10;
	
	//Override
	public As2BenchTxnType getTxnType() {
		return As2BenchTxnType.UPDATE_ITEM;
	}
	//Override
	public Object[] generateParameter() {
		RandomValueGenerator rvg = new RandomValueGenerator();
		LinkedList<Object> paramList = new LinkedList<Object>();
		
		paramList.add(UPDATE_COUNT);
		for (int i = 0; i < UPDATE_COUNT; i++)
			paramList.add(rvg.number(1, As2BenchConstants.NUM_ITEMS));
		for (int i = 0; i < UPDATE_COUNT; i++)
			paramList.add(rvg.randomDoubleIncrRange(0.0,5.0,0.1));
		return paramList.toArray();
	}
}

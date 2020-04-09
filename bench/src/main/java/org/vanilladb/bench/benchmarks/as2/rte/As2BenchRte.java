/*******************************************************************************
 * Copyright 2016, 2018 vanilladb.org contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package org.vanilladb.bench.benchmarks.as2.rte;

import org.vanilladb.bench.StatisticMgr;
import org.vanilladb.bench.benchmarks.as2.As2BenchTxnType;
import org.vanilladb.bench.remote.SutConnection;
import org.vanilladb.bench.rte.RemoteTerminalEmulator;

//import this for read the read/write ratio specified in vanillabench.property
import org.vanilladb.bench.util.BenchProperties;

public class As2BenchRte extends RemoteTerminalEmulator<As2BenchTxnType> {
	
	private As2BenchTxExecutor executor;
	
	//Add a As2BenchTxExecutor for UPDATE_ITEM
	private As2BenchTxExecutor executor_for_update;
	
	//Record read/write ratio
	public static final double READ_RATIO = BenchProperties.getLoader().getPropertyAsDouble(As2BenchRte.class.getName() + ".READ_RATIO", 1.0);
	public static final double WRITE_RATIO = BenchProperties.getLoader().getPropertyAsDouble(As2BenchRte.class.getName() + ".WRITE_RATIO", 0.0);
	//A counter for decide which Transaction should be done base on the read write ratio
	private int counter;
	public As2BenchRte(SutConnection conn, StatisticMgr statMgr) {
		super(conn, statMgr);
		executor = new As2BenchTxExecutor(new As2ReadItemParamGen());
		executor_for_update = new As2BenchTxExecutor(new As2UpdateItemParamGen());
		counter = 0;
	}
	
	protected As2BenchTxnType getNextTxType() {
		int read_times = (int)(READ_RATIO*10);
		/*int write_times = (int)(WRITE_RATIO*10);*/
		if(READ_RATIO == 1.0) {
			return As2BenchTxnType.READ_ITEM;
		}
		else if(WRITE_RATIO == 1.0) {
			return As2BenchTxnType.UPDATE_ITEM;
		}
		else if(counter < read_times) {
			counter++;
			return As2BenchTxnType.READ_ITEM;
		}
		else {
			//for write type transactions
			counter++;
			counter = counter % 10;
			return As2BenchTxnType.UPDATE_ITEM;
		}
		/*else if(counter-read_times < write_times-1) {
			counter++;
			return As2BenchTxnType.UPDATE_ITEM;
		}
		else {
			counter = 0;
			return As2BenchTxnType.UPDATE_ITEM;
		}*/
	}
	
	protected As2BenchTxExecutor getTxExeutor(As2BenchTxnType type) {
	
		if (type == As2BenchTxnType.READ_ITEM)
			return executor;
		else if(type == As2BenchTxnType.UPDATE_ITEM)
			return executor_for_update;
		else
			return executor;
	}
}

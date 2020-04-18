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
package org.vanilladb.bench;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.Arrays;

import org.vanilladb.bench.util.BenchProperties;

public class StatisticMgr {
	private static Logger logger = Logger.getLogger(StatisticMgr.class.getName());

	private static final File OUTPUT_DIR;


	static {
		String outputDirPath = BenchProperties.getLoader().getPropertyAsString(StatisticMgr.class.getName()
				+ ".OUTPUT_DIR", null);
		
		if (outputDirPath == null) {
			OUTPUT_DIR = new File(System.getProperty("user.home"), "benchmark_results");
		} else {
			OUTPUT_DIR = new File(outputDirPath);
		}

		// Create the directory if that doesn't exist
		if (!OUTPUT_DIR.exists())
			OUTPUT_DIR.mkdir();
		
		
	}

	private static class TxnStatistic {
		private BenchTransactionType mType;
		private int txnCount = 0;
		private long totalResponseTimeNs = 0;

		public TxnStatistic(BenchTransactionType txnType) {
			this.mType = txnType;
		}

		public BenchTransactionType getmType() {
			return mType;
		}

		public void addTxnResponseTime(long responseTime) {
			txnCount++;
			totalResponseTimeNs += responseTime;
		}

		public int getTxnCount() {
			return txnCount;
		}

		public long getTotalResponseTime() {
			return totalResponseTimeNs;
		}
	}

	private List<TxnResultSet> resultSets = new ArrayList<TxnResultSet>();
	private List<BenchTransactionType> allTxTypes;
	private String fileNamePostfix = "";
	private long recordStartTime = -1;
	
	public StatisticMgr(Collection<BenchTransactionType> txTypes) {
		allTxTypes = new LinkedList<BenchTransactionType>(txTypes);
	}
	
	public StatisticMgr(Collection<BenchTransactionType> txTypes, String namePostfix) {
		allTxTypes = new LinkedList<BenchTransactionType>(txTypes);
		fileNamePostfix = namePostfix;
	}
	
	/**
	 * We use the time that this method is called at as the start time for recording.
	 */
	public synchronized void setRecordStartTime() {
		if (recordStartTime == -1)
			recordStartTime = System.nanoTime();
	}

	public synchronized void processTxnResult(TxnResultSet trs) {
		if (recordStartTime == -1)
			recordStartTime = trs.getTxnEndTime();
		resultSets.add(trs);
	}

	public synchronized void outputReport() {
		try {
			SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd-HHmmss"); // E.g. "20180524-200824"
			String fileName = formatter.format(Calendar.getInstance().getTime());
			if (fileNamePostfix != null && !fileNamePostfix.isEmpty())
				fileName += "-" + fileNamePostfix; // E.g. "20200524-200824-postfix"
			
			outputDetailReport(fileName);
			outputCSVReport(fileName);
			
			
		} catch (IOException e) {
			e.printStackTrace();
		}

		if (logger.isLoggable(Level.INFO))
			logger.info("Finnish creating tpcc benchmark report");
	}
	
	private void outputDetailReport(String fileName) throws IOException {
		Map<BenchTransactionType, TxnStatistic> txnStatistics = new HashMap<BenchTransactionType, TxnStatistic>();
		Map<BenchTransactionType, Integer> abortedCounts = new HashMap<BenchTransactionType, Integer>();
		
		for (BenchTransactionType type : allTxTypes) {
			txnStatistics.put(type, new TxnStatistic(type));
			abortedCounts.put(type, 0);
		}
		
		try (BufferedWriter writer = new BufferedWriter(new FileWriter(new File(OUTPUT_DIR, fileName + ".txt")))) {
			// First line: total transaction count
			writer.write("# of txns (including aborted) during benchmark period: " + resultSets.size());
			writer.newLine();
			
			// Detail latency report
			for (TxnResultSet resultSet : resultSets) {
				if (resultSet.isTxnIsCommited()) {
					// Write a line: {[Tx Type]: [Latency]}
					/*writer.write(resultSet.getTxnType() + ": "
							+ TimeUnit.NANOSECONDS.toMillis(resultSet.getTxnResponseTime()) + " ms");*/
					writer.write(resultSet.getTxnType() + ": "
							+ TimeUnit.NANOSECONDS.toMicros(resultSet.getTxnResponseTime()) + " us");
					writer.newLine();
					
					// Count transaction for each type
					TxnStatistic txnStatistic = txnStatistics.get(resultSet.getTxnType());
					txnStatistic.addTxnResponseTime(resultSet.getTxnResponseTime());
					
					
				} else {
					writer.write(resultSet.getTxnType() + ": ABORTED");
					writer.newLine();
					
					// Count transaction for each type
					Integer count = abortedCounts.get(resultSet.getTxnType());
					abortedCounts.put(resultSet.getTxnType(), count + 1);
				}
			}
			writer.newLine();
			
			// Last few lines: show the statistics for each type of transactions
			int abortedTotal = 0;
			for (Entry<BenchTransactionType, TxnStatistic> entry : txnStatistics.entrySet()) {
				TxnStatistic value = entry.getValue();
				int abortedCount = abortedCounts.get(entry.getKey());
				abortedTotal += abortedCount;
				//long avgResTimeMs = 0;
				long avgResTimeUs = 0L;
				//long avgResTimeNs = 0;
				
				if (value.txnCount > 0) {
					/*avgResTimeMs = TimeUnit.NANOSECONDS.toMillis(
							value.getTotalResponseTime() / value.txnCount);*/
					avgResTimeUs = TimeUnit.NANOSECONDS.toMicros(value.getTotalResponseTime() / value.txnCount);
					//avgResTimeNs = value.getTotalResponseTime() / value.txnCount;
				}
				
				/*writer.write(value.getmType() + " - committed: " + value.getTxnCount() +
						", aborted: " + abortedCount + ", avg latency: " + avgResTimeMs + " ms");*/
				writer.write(value.getmType() + " - committed: " + value.getTxnCount() +
				", aborted: " + abortedCount + ", avg latency: " + avgResTimeUs + " us");
				/*writer.write(value.getmType() + " - committed: " + value.getTxnCount() +
						", aborted: " + abortedCount + ", avg latency: " + avgResTimeNs + " ns");*/
				writer.newLine();
			}
			
			/*// Last line: Total statistics
			int finishedCount = resultSets.size() - abortedTotal;
			//double avgResTimeMs = 0;
			double avgResTimeUs = 0;
			if (finishedCount > 0) { // Avoid "Divide By Zero"
				for (TxnResultSet rs : resultSets)
					avgResTimeUs += rs.getTxnResponseTime() / finishedCount;
			}
			writer.write(String.format("TOTAL - committed: %d, aborted: %d, avg latency: %d us", 
					finishedCount, abortedTotal, Math.round(avgResTimeUs / 1000)));*/
			
			// Last line: Total statistics
			int finishedCount = resultSets.size() - abortedTotal;
			//double avgResTimeMs = 0;
			long avgResTimeUs = 0L;
			if (finishedCount > 0) { // Avoid "Divide By Zero"
				for (TxnResultSet rs : resultSets)
					avgResTimeUs += TimeUnit.NANOSECONDS.toMicros(rs.getTxnResponseTime());
			}
			avgResTimeUs = avgResTimeUs / finishedCount;
			writer.write(String.format("TOTAL - committed: %d, aborted: %d, avg latency: %d us", 
								finishedCount, abortedTotal, avgResTimeUs));
		}
	}
	
	private void outputCSVReport(String fileName) throws IOException{
		
		
		try (BufferedWriter writer = new BufferedWriter(new FileWriter(new File(OUTPUT_DIR, fileName + ".csv")))) {
			// First line:  units
			writer.write("time(sec), throughput(txs), avg_latency(us), min(us), max(us), 25th_lat(us), median_lat(us), 75th_lat(us)");
			writer.newLine();
			
			// Detail latency report
			int counted_txn = 0;
			int time_sec = 0;
			long period_Nanosecs = 5000000000L;
			while(counted_txn < resultSets.size()) {
				if(resultSets.get(counted_txn).isTxnIsCommited()) {
					List<TxnResultSet> in_period_resultSets = new ArrayList<TxnResultSet>();
					in_period_resultSets.add(resultSets.get(counted_txn));
					long period_start = resultSets.get(counted_txn).getTxnEndTime();
					long period_end = resultSets.get(counted_txn).getTxnEndTime();
					counted_txn++;
					while(period_end - period_start <= period_Nanosecs && counted_txn < resultSets.size()){
						if(resultSets.get(counted_txn).isTxnIsCommited()) {
							//in_period_resultSets.add(resultSets.get(counted_txn));
							period_end = resultSets.get(counted_txn).getTxnEndTime();
							if(period_end - period_start > period_Nanosecs) {
								break;
							}
							else {
								in_period_resultSets.add(resultSets.get(counted_txn));
								counted_txn++;
							}
							
						}
						else {
							counted_txn++;
						}
					}
					long [] restime_in_period = new long[in_period_resultSets.size()];
					long totalResTimeUs = 0L;
					long avgResTimeUs = 0L;
					long minResTimeUs= 0L;
					long maxResTimeUs = 0L;
					long first_quar_ResTimeUs = 0L;
					long median_ResTimeUs = 0L;
					long third_quar_ResTimeUs = 0L;
					for(int i = 0; i < in_period_resultSets.size(); i++) {
						restime_in_period[i] = in_period_resultSets.get(i).getTxnResponseTime();
						totalResTimeUs += in_period_resultSets.get(i).getTxnResponseTime();
					}
					Arrays.sort(restime_in_period);
					
					
					
					//decide avg latency
					avgResTimeUs = TimeUnit.NANOSECONDS.toMicros(totalResTimeUs / in_period_resultSets.size());
					//decide min 
					minResTimeUs = TimeUnit.NANOSECONDS.toMicros(restime_in_period[0]);
					//decide max
					maxResTimeUs = TimeUnit.NANOSECONDS.toMicros(restime_in_period[in_period_resultSets.size()-1]);
						
					if(in_period_resultSets.size() % 2 == 0) {
						//pivot shows the index of the median number, for example {3,5,7,9} pivot = 1, median = (5+7) / 2
						int pivot = in_period_resultSets.size()/2 - 1;
						//decide median
						median_ResTimeUs = TimeUnit.NANOSECONDS.toMicros((restime_in_period[pivot] + restime_in_period[pivot+1])/2);
						
						if(in_period_resultSets.size()%4 == 0 ) {
							pivot = in_period_resultSets.size()/4 - 1;
							//decide 25th latency
							first_quar_ResTimeUs = TimeUnit.NANOSECONDS.toMicros((restime_in_period[pivot] + restime_in_period[pivot+1])/2);
							//decide 75th latency
							pivot = in_period_resultSets.size()*3/4 - 1;
							third_quar_ResTimeUs = TimeUnit.NANOSECONDS.toMicros((restime_in_period[pivot] + restime_in_period[pivot+1])/2);
							
						}
						else {
							//decide 25th latency
							pivot = in_period_resultSets.size()/4;
							first_quar_ResTimeUs = TimeUnit.NANOSECONDS.toMicros(restime_in_period[pivot]);
							//decide 75th latency
							pivot = in_period_resultSets.size()*3/4;
							third_quar_ResTimeUs = TimeUnit.NANOSECONDS.toMicros(restime_in_period[pivot]);
						}
					
					
					}
					else if(in_period_resultSets.size() > 1){
						int pivot = in_period_resultSets.size()/2;
						//decide median
						median_ResTimeUs = TimeUnit.NANOSECONDS.toMicros(restime_in_period[pivot]);
						if((in_period_resultSets.size()-1)%4 == 0) {
							//decide 25th latency
							pivot = in_period_resultSets.size()/4 - 1;
							first_quar_ResTimeUs = TimeUnit.NANOSECONDS.toMicros((restime_in_period[pivot] + restime_in_period[pivot+1])/2);
							//decide 75th latency
							pivot = in_period_resultSets.size()*3/4;
							third_quar_ResTimeUs = TimeUnit.NANOSECONDS.toMicros((restime_in_period[pivot] + restime_in_period[pivot+1])/2);
							
						}
						else{
							//decide 25th latency
							pivot = in_period_resultSets.size()/4;
							first_quar_ResTimeUs = TimeUnit.NANOSECONDS.toMicros(restime_in_period[pivot]);
							//decide 75th latency
							pivot = in_period_resultSets.size()*3/4;
							third_quar_ResTimeUs = TimeUnit.NANOSECONDS.toMicros(restime_in_period[pivot]);
						}
					}
					else {
						int pivot = 0;
						median_ResTimeUs = TimeUnit.NANOSECONDS.toMicros(restime_in_period[pivot]);
						first_quar_ResTimeUs = TimeUnit.NANOSECONDS.toMicros(restime_in_period[pivot]);
						third_quar_ResTimeUs = TimeUnit.NANOSECONDS.toMicros(restime_in_period[pivot]);
						
					}
					time_sec += 5;
					writer.write(time_sec + ", " + in_period_resultSets.size() + ", " + avgResTimeUs + ", "
							+ minResTimeUs + ", " + maxResTimeUs + ", " + first_quar_ResTimeUs + ", " + median_ResTimeUs + ", " + third_quar_ResTimeUs);
					writer.newLine();
					
					
				}
				else {
					counted_txn++;
				}
			}
		}	
		
	}
	
	
}
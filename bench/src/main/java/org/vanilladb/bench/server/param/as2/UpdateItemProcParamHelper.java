package org.vanilladb.bench.server.param.as2;

import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.storedprocedure.SpResultRecord;
import org.vanilladb.core.sql.storedprocedure.StoredProcedureParamHelper;

public class UpdateItemProcParamHelper extends StoredProcedureParamHelper {

	//Parameters
	private int updateCount;
	private int[] updateItemId;
	private double[] updateItemPrice;
	
	public int getUpdateCount() {
		return updateCount;
	}

	public int getUpdateItemId(int index) {
		return updateItemId[index];
	}
	
	public double getUpdateItemPrice(int index) {
		return updateItemPrice[index];
	}
	@Override
	public void prepareParameters(Object... pars) {
		// TODO Auto-generated method stub
		int indexCnt = 0;
		
		updateCount = (Integer)pars[indexCnt++];
		updateItemId = new int[updateCount];
		updateItemPrice = new double[updateCount];
		
		for (int i = 0; i < updateCount; i++)
			updateItemId[i] = (Integer) pars[indexCnt++];
		for (int i = 0; i < updateCount; i++)
			updateItemPrice[i] = (Double) pars[indexCnt++];

	}

	@Override
	public Schema getResultSetSchema() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public SpResultRecord newResultSetRecord() {
		// TODO Auto-generated method stub
		return null;
	}

}

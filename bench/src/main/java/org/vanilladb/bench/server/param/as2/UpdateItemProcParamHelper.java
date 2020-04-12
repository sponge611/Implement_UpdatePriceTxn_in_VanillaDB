package org.vanilladb.bench.server.param.as2;

import org.vanilladb.core.sql.DoubleConstant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.sql.VarcharConstant;
import org.vanilladb.core.sql.storedprocedure.SpResultRecord;
import org.vanilladb.core.sql.storedprocedure.StoredProcedureParamHelper;

public class UpdateItemProcParamHelper extends StoredProcedureParamHelper {

	//Parameters
	private int updateCount;
	private int[] updateItemId;
	private double[] updateItemPrice;
	
	private String[] itemName;
	private double[] itemPrice;
	
	public int getUpdateCount() {
		return updateCount;
	}

	public int getUpdateItemId(int index) {
		return updateItemId[index];
	}
	
	public double getUpdateItemPrice(int index) {
		return updateItemPrice[index];
	}
	
	public void setItemName(String s, int idx) {
		itemName[idx] = s;
	}

	public void setItemPrice(double d, int idx) {
		itemPrice[idx] = d;
	}
	@Override
	public void prepareParameters(Object... pars) {
		// TODO Auto-generated method stub
		int indexCnt = 0;
		
		updateCount = (Integer)pars[indexCnt++];
		updateItemId = new int[updateCount];
		updateItemPrice = new double[updateCount];
		itemName = new String[updateCount];
		itemPrice = new double[updateCount];
		
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
		SpResultRecord rec = new SpResultRecord();
		rec.setVal("rc", new IntegerConstant(itemName.length));
		for (int i = 0; i < itemName.length; i++) {
			rec.setVal("i_name_" + i, new VarcharConstant(itemName[i], Type.VARCHAR(24)));
			rec.setVal("i_price_" + i, new DoubleConstant(itemPrice[i]));
		}
		return rec;
	}

}

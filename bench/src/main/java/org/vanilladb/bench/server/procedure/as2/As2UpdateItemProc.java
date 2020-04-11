package org.vanilladb.bench.server.procedure.as2;


import org.vanilladb.bench.benchmarks.as2.As2BenchConstants;
import org.vanilladb.bench.server.param.as2.UpdateItemProcParamHelper;
import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.storedprocedure.StoredProcedure;
import org.vanilladb.core.storage.tx.Transaction;

public class As2UpdateItemProc extends StoredProcedure<UpdateItemProcParamHelper> {
	
	public As2UpdateItemProc() {
		super(new UpdateItemProcParamHelper());
	}
	
	protected void executeSql() {
		UpdateItemProcParamHelper paramHelper = getParamHelper();
		Transaction tx = getTransaction();
		double origin_price = 0.00;
		double update_price = 0.00;
		for(int idx = 0; idx < paramHelper.getUpdateCount(); idx++) {
			int iid = paramHelper.getUpdateItemId(idx);
			double raise_price = paramHelper.getUpdateItemPrice(idx);
			Plan p = VanillaDb.newPlanner().createQueryPlan(
					"SELECT i_price FROM item WHERE i_id = " + iid, tx);
			Scan s = p.open();
			
			s.beforeFirst();
			if (s.next()) {
				
				origin_price = (Double) s.getVal("i_price").asJavaVal();
				update_price = origin_price + raise_price;
				if(origin_price > As2BenchConstants.MAX_PRICE) {
					if(VanillaDb.newPlanner().executeUpdate(
							"UPDATE item SET i_price = " + As2BenchConstants.MIN_PRICE + " WHERE i_id = " + iid, tx) != 1) {
						throw new RuntimeException("Update Item Price Fail: Could not update item with i_id = " + iid);
					}
				}
				else {
					if(VanillaDb.newPlanner().executeUpdate(
							"UPDATE item SET i_price = " + update_price + " WHERE i_id = " + iid, tx) != 1) {
						throw new RuntimeException("Update Item Price Fail: Could not update item with i_id = " + iid);
					}
					
				}
			} else
				throw new RuntimeException("Cloud not find item record with i_id = " + iid + ", so cannot update it");

			s.close();
		}
	}
}

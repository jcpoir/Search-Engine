package cis5550.flame;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Serializer;

public class FlamePairRDDImpl implements FlamePairRDD{
	private String tableName;
	private KVSClient client;
	private String masterArg;

	public FlamePairRDDImpl(String name, String masterArg) {
		this.tableName = name;
		this.masterArg = masterArg;
		this.client = new KVSClient(masterArg);
	}

	@Override
	public List<FlamePair> collect() throws Exception {
		Iterator<Row> iterator = client.scan(tableName);
		List<FlamePair> list = new ArrayList<>();
		while (iterator.hasNext()) {
			Row getRow = iterator.next();
			for (String col : getRow.columns()) {
				list.add(new FlamePair(getRow.key(), getRow.get(col)));
			}
		}

		return list;
	}

	@Override
	public FlamePairRDD foldByKey(String zeroElement, TwoStringsToString lambda) throws Exception {
		String newTableName = FlameContextImpl.invokeOperation("POST", "/rdd/foldByKey",
				Serializer.objectToByteArray(lambda), tableName, zeroElement);

		FlamePairRDD newFlameRDD = new FlamePairRDDImpl(newTableName, masterArg);
		return newFlameRDD;
	}
  
}

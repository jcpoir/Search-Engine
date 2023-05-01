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

	public String getTableName() {
		return tableName;
	}

	public int count() throws Exception {
		return client.count(tableName);
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

	@Override
	public void saveAsTable(String tableNameArg) throws Exception {
		client.rename(tableName, tableNameArg);
		this.tableName = tableNameArg;
	}

	@Override
	public FlameRDD flatMap(PairToStringIterable lambda) throws Exception {
		String newTableName = FlameContextImpl.invokeOperation("POST", "/rdd/pairFlatMap", Serializer.objectToByteArray(lambda),
				tableName, null);

		FlameRDDImpl newFlameRDD = new FlameRDDImpl(newTableName, masterArg);
		return newFlameRDD;
	}

	@Override
	public FlamePairRDD flatMapToPair(PairToPairIterable lambda) throws Exception {
		String newTableName = FlameContextImpl.invokeOperation("POST", "/rdd/pairFlatMapToPair",
				Serializer.objectToByteArray(lambda), tableName, null);

		FlamePairRDD newFlameRDD = new FlamePairRDDImpl(newTableName, masterArg);
		return newFlameRDD;
	}

	@Override
	public FlamePairRDD join(FlamePairRDD other) throws Exception {
		FlamePairRDDImpl impl = (FlamePairRDDImpl) other;
		String newTableName = FlameContextImpl.invokeOperation("POST", "/rdd/join",
				null, tableName, impl.getTableName());

		FlamePairRDD newFlameRDD = new FlamePairRDDImpl(newTableName, masterArg);
		return newFlameRDD;
	}

	@Override
	public FlamePairRDD cogroup(FlamePairRDD other) throws Exception {
		FlamePairRDDImpl impl = (FlamePairRDDImpl) other;
		String newTableName = FlameContextImpl.invokeOperation("POST", "/rdd/join",
				null, tableName, impl.getTableName());

		FlamePairRDD newFlameRDD = new FlamePairRDDImpl(newTableName, masterArg);
		return newFlameRDD;
	}
  
}

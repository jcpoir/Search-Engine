package cis5550.flame;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Serializer;

public class FlameRDDImpl implements FlameRDD {
	private String tableName;
	private KVSClient client;
	private String masterArg;

	public FlameRDDImpl(String name, String masterArg) {
		this.tableName = name;
		this.masterArg = masterArg;
		this.client = new KVSClient(masterArg);
	}

	public String getTableName() {
		return tableName;
	}

	@Override
	public List<String> collect() throws Exception {

		Iterator<Row> iterator = client.scan(tableName);
		List<String> list = new ArrayList<>();
		while (iterator.hasNext()) {
			Row getRow = iterator.next();
			list.add(getRow.get("value"));
		}

		return list;
	}

	@Override
	public FlameRDD flatMap(StringToIterable lambda) throws Exception {
		String newTableName = FlameContextImpl.invokeOperation("POST", "/rdd/flatMap", Serializer.objectToByteArray(lambda), 
			tableName, null);
		
		FlameRDDImpl newFlameRDD = new FlameRDDImpl(newTableName, masterArg);
		return newFlameRDD;
	}

	@Override
	public FlamePairRDD mapToPair(StringToPair lambda) throws Exception {
		String newTableName = FlameContextImpl.invokeOperation("POST", "/rdd/mapToPair", Serializer.objectToByteArray(lambda),
				tableName, null);

		FlamePairRDD newFlameRDD = new FlamePairRDDImpl(newTableName, masterArg);
		return newFlameRDD;

	}

	@Override
	public FlameRDD intersection(FlameRDD r) throws Exception {
		FlameRDDImpl impl = (FlameRDDImpl) r;
		String convertOriginalTable = FlameContextImpl.invokeOperation("POST", "/rdd/convert", 
				null, tableName, null);
		String convertRTable = FlameContextImpl.invokeOperation("POST", "/rdd/convert",
				null, impl.getTableName(), null);
		
		String newTableName = FlameContextImpl.invokeOperation("POST", "/rdd/intersection", convertRTable.getBytes(),
				convertOriginalTable, null);

		FlameRDDImpl newFlameRDD = new FlameRDDImpl(newTableName, masterArg);
		return newFlameRDD;
	}

	@Override
	public FlameRDD sample(double f) throws Exception {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(bos);
		dos.writeDouble(f);
		dos.flush();
		byte[] fBytes = bos.toByteArray();

		String newTableName = FlameContextImpl.invokeOperation("POST", "/rdd/sample", fBytes,
				tableName, null);

		FlameRDDImpl newFlameRDD = new FlameRDDImpl(newTableName, masterArg);
		return newFlameRDD;
	}

	@Override
	public FlamePairRDD groupBy(StringToString lambda) throws Exception {
		String newTableName = FlameContextImpl.invokeOperation("POST", "/rdd/groupBy", Serializer.objectToByteArray(lambda),
				tableName, null);

		FlamePairRDDImpl newFlameRDD = new FlamePairRDDImpl(newTableName, masterArg);
		return newFlameRDD;
	}
  
}

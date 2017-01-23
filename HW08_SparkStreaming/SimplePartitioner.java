package kafka.streaming;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

public class SimplePartitioner implements Partitioner {
	public SimplePartitioner (VerifiableProperties props) {

	}

	public int partition(Object number, int numPartitions) {
		int partition = 0;
		
		String str = (String) number;
		int key = Integer.parseInt(str);
		
		if (key > 0) {
			partition = key % numPartitions;
		}
		
		return partition;
	}

}

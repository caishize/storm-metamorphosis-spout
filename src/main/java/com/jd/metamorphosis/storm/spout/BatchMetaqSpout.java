package com.jd.metamorphosis.storm.spout;

import backtype.storm.spout.RawScheme;
import backtype.storm.spout.Scheme;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.LinkedList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import storm.trident.operation.TridentCollector;
import storm.trident.spout.IOpaquePartitionedTridentSpout;
import storm.trident.topology.TransactionAttempt;

import com.jd.metamorphosis.util.MetaqReceiver;
import com.taobao.gecko.core.util.LinkedTransferQueue;
import com.taobao.metamorphosis.client.MetaClientConfig;
import com.taobao.metamorphosis.client.consumer.ConsumerConfig;

/**
 * 
 * @author darwin <caishize@outlook.com>
 * @date 2012-12-20
 * 
 */

@SuppressWarnings("serial")
public class BatchMetaqSpout implements IOpaquePartitionedTridentSpout<Object> {

	private final MetaClientConfig metaClientConfig;

	private final ConsumerConfig consumerConfig;

	static final Log log = LogFactory.getLog(BatchMetaqSpout.class);

	private final Scheme _scheme;

	public BatchMetaqSpout(final MetaClientConfig metaClientConfig,
			final ConsumerConfig consumerConfig, final Scheme scheme) {
		super();
		this.metaClientConfig = metaClientConfig;
		this.consumerConfig = consumerConfig;
		this._scheme = scheme;
	}

	public BatchMetaqSpout(final MetaClientConfig metaClientConfig,
			final ConsumerConfig consumerConfig) {
		this(metaClientConfig, consumerConfig, new RawScheme());
	}

	public class MetaqCoordinator implements
			IOpaquePartitionedTridentSpout.Coordinator {

		@Override
		public boolean isReady(long txid) {
			return true;
		}

		@Override
		public void close() {

		}

	}

	public class MetaqEmitter implements
			IOpaquePartitionedTridentSpout.Emitter<Object> {
		private static final int DEFAULT_NUM_PARTITIONS = 4;
		private static final String NUMPARTITIONS = "meta.num.partitions";
		LinkedTransferQueue<MetaqMessageWrapper> messageQueue;
		MetaqReceiver _receiver;
		List<MetaqMessageWrapper> _buffer;
		int numPartitions;

		public MetaqEmitter(@SuppressWarnings("rawtypes") Map conf,
				TopologyContext context) {
			_buffer = new ArrayList<MetaqMessageWrapper>();
			messageQueue = new LinkedTransferQueue<MetaqMessageWrapper>();
			_receiver = new MetaqReceiver(conf,
					BatchMetaqSpout.this.metaClientConfig,
					BatchMetaqSpout.this.consumerConfig, this.messageQueue);
			Long tmpNumPartitions = (Long) conf.get(NUMPARTITIONS);
			if (tmpNumPartitions == null) {
				log.warn("Using default DEFAULT_NUM_PARTITIONS");
				numPartitions = DEFAULT_NUM_PARTITIONS;
			} else {
				numPartitions = Integer.parseInt(Long
						.toString(tmpNumPartitions));
			}

		}

		@Override
		public void close() {
		}

		@Override
		public Object emitPartitionBatch(TransactionAttempt tx,
				TridentCollector collector, int partition,
				Object lastPartitionMeta) {
			_buffer.clear();
			messageQueue.drainTo(_buffer);
			for (MetaqMessageWrapper elem : _buffer) {
				LinkedList<Object> values = new LinkedList<Object>(
						BatchMetaqSpout.this._scheme.deserialize(elem.message
								.getData()));
				values.addFirst(tx);
				collector.emit(values);
			}
			return null;
		}

		@Override
		public long numPartitions() {
			return numPartitions;
		}

	}

	@Override
	public Emitter<Object> getEmitter(@SuppressWarnings("rawtypes") Map conf,
			TopologyContext context) {
		return new MetaqEmitter(conf, context);
	}

	@Override
	public IOpaquePartitionedTridentSpout.Coordinator getCoordinator(
			@SuppressWarnings("rawtypes") Map conf, TopologyContext context) {
		return new MetaqCoordinator();
	}

	@SuppressWarnings("rawtypes")
	@Override
	public Map getComponentConfiguration() {
		return null;
	}

	@Override
	public Fields getOutputFields() {
		return new Fields("txid", "message");
	}
}

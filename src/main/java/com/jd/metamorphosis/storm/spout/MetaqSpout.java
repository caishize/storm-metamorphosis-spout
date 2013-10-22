package com.jd.metamorphosis.storm.spout;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import backtype.storm.spout.RawScheme;
import backtype.storm.spout.Scheme;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;

import com.jd.metamorphosis.util.Constants;
import com.jd.metamorphosis.util.MetaqReceiver;
import com.taobao.gecko.core.util.LinkedTransferQueue;
import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.MetaClientConfig;
import com.taobao.metamorphosis.client.consumer.ConsumerConfig;

/**
 * 
 * @author modified by darwin <caishize@outlook.com>
 * @date 2012-12-05
 * 
 */
@SuppressWarnings("serial")
public class MetaqSpout extends BaseRichSpout {

	private final MetaClientConfig metaClientConfig;

	private final ConsumerConfig consumerConfig;

	static final Log log = LogFactory.getLog(MetaqSpout.class);

	private final Scheme scheme;

	private MetaqReceiver _receiver;

	/**
	 * Time in milliseconds to wait for a message from the queue if there is no
	 * message ready when the topology requests a tuple (via
	 * {@link #nextTuple()}).
	 */

	private boolean ack = false;

	private transient SpoutOutputCollector collector;

	private transient LinkedTransferQueue<MetaqMessageWrapper> messageQueue;

	public MetaqSpout(final MetaClientConfig metaClientConfig,
			final ConsumerConfig consumerConfig, final Scheme scheme) {
		super();
		this.metaClientConfig = metaClientConfig;
		this.consumerConfig = consumerConfig;
		this.scheme = scheme;
	}

	public MetaqSpout(final MetaClientConfig metaClientConfig,
			final ConsumerConfig consumerConfig) {
		this(metaClientConfig, consumerConfig, new RawScheme());
	}

	@Override
	public void open(@SuppressWarnings("rawtypes") final Map conf,
			final TopologyContext context, final SpoutOutputCollector collector) {

		this.messageQueue = new LinkedTransferQueue<MetaqMessageWrapper>();
		this.collector = collector;
		_receiver = new MetaqReceiver(conf, this.metaClientConfig,
				this.consumerConfig, this.messageQueue);
		final Boolean ack = (Boolean) conf.get(Constants.ACK);
		if (ack != null) {
			this.ack = ack;
		}
	}

	@Override
	public void close() {
		_receiver.close();

	}

	@Override
	public void nextTuple() {
		try {
			final MetaqMessageWrapper wrapper = this.messageQueue.poll(
					Constants.WAIT_FOR_NEXT_MESSAGE, TimeUnit.MILLISECONDS);
			if (wrapper == null) {
				return;
			}
			final Message message = wrapper.message;
			if (ack) {
				this.collector.emit(this.scheme.deserialize(message.getData()),
						message.getId());
			} else {
				this.collector.emit(this.scheme.deserialize(message.getData()));
			}
		} catch (final InterruptedException e) {
			// interrupted while waiting for message, big deal
			log.error("There is something wrong in polling message from the queue"
					+ e);
		}
	}

	@Override
	public void activate() {
	}

	@Override
	public void deactivate() {
	}

	@Override
	public void ack(final Object msgId) {
		if (ack) {
			if (msgId instanceof Long) {
				final long id = (Long) msgId;
				_receiver.ack(id);
			} else {
				log.warn(String.format("don't know how to ack(%s: %s)", msgId
						.getClass().getName(), msgId));
			}
		}

	}

	@Override
	public void fail(final Object msgId) {
		if (ack) {
			if (msgId instanceof Long) {
				final long id = (Long) msgId;
				_receiver.fail(id);
			} else {
				log.warn(String.format("don't know how to reject(%s: %s)",
						msgId.getClass().getName(), msgId));
			}
		}

	}

	@Override
	public void declareOutputFields(final OutputFieldsDeclarer declarer) {
		declarer.declare(this.scheme.getOutputFields());
	}

}
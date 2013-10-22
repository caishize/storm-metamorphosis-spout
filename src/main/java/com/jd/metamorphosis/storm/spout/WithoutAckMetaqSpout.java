package com.jd.metamorphosis.storm.spout;

import java.util.Map;
import java.util.concurrent.Executor;
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
import com.taobao.gecko.core.util.LinkedTransferQueue;
import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.MessageSessionFactory;
import com.taobao.metamorphosis.client.MetaClientConfig;
import com.taobao.metamorphosis.client.MetaMessageSessionFactory;
import com.taobao.metamorphosis.client.consumer.ConsumerConfig;
import com.taobao.metamorphosis.client.consumer.MessageConsumer;
import com.taobao.metamorphosis.client.consumer.MessageListener;
import com.taobao.metamorphosis.exception.MetaClientException;

/**
 * 
 * @author modified by darwin <caishize@outlook.com>
 * @date 2012-12-05
 * 
 */
@SuppressWarnings("serial")
public class WithoutAckMetaqSpout extends BaseRichSpout {

	private final MetaClientConfig metaClientConfig;

	private final ConsumerConfig consumerConfig;

	private transient MessageConsumer messageConsumer;

	private transient MessageSessionFactory sessionFactory;

	static final Log log = LogFactory.getLog(WithoutAckMetaqSpout.class);

	private final Scheme scheme;

	/**
	 * Time in milliseconds to wait for a message from the queue if there is no
	 * message ready when the topology requests a tuple (via
	 * {@link #nextTuple()}).
	 */
	public static final long WAIT_FOR_NEXT_MESSAGE = 1L;

	private transient SpoutOutputCollector collector;

	private transient LinkedTransferQueue<Message> messageQueue;

	public WithoutAckMetaqSpout(final MetaClientConfig metaClientConfig,
			final ConsumerConfig consumerConfig, final Scheme scheme) {
		super();
		this.metaClientConfig = metaClientConfig;
		this.consumerConfig = consumerConfig;
		this.scheme = scheme;
	}

	public WithoutAckMetaqSpout(final MetaClientConfig metaClientConfig,
			final ConsumerConfig consumerConfig) {
		this(metaClientConfig, consumerConfig, new RawScheme());
	}

	public void open(@SuppressWarnings("rawtypes") final Map conf,
			final TopologyContext context, final SpoutOutputCollector collector) {
		final String topic = (String) conf.get(Constants.TOPIC);
		if (topic == null) {
			throw new IllegalArgumentException(Constants.TOPIC + " is null");
		}
		Integer maxSize = Integer.parseInt(Long.toString((Long) conf
				.get(Constants.FETCH_MAX_SIZE)));
		if (maxSize == null) {
			log.warn("Using default FETCH_MAX_SIZE");
			maxSize = Constants.DEFAULT_MAX_SIZE;
		}
		this.messageQueue = new LinkedTransferQueue<Message>();
		try {
			this.collector = collector;
			this.setUpMeta(topic, maxSize);
		} catch (final MetaClientException e) {
			log.error("Setup meta consumer failed", e);
		}
	}

	private void setUpMeta(final String topic, final Integer maxSize)
			throws MetaClientException {
		this.sessionFactory = new MetaMessageSessionFactory(
				this.metaClientConfig);
		this.messageConsumer = this.sessionFactory
				.createConsumer(this.consumerConfig);
		this.messageConsumer.subscribe(topic, maxSize, new MessageListener() {

			public void recieveMessages(final Message message) {
				WithoutAckMetaqSpout.this.messageQueue.offer(message);
			}

			public Executor getExecutor() {
				return null;
			}
		}).completeSubscribe();
	}

	public void close() {
		try {
			this.messageConsumer.shutdown();
		} catch (final MetaClientException e) {
			log.error("Shutdown consumer failed", e);
		}
		try {
			this.sessionFactory.shutdown();
		} catch (final MetaClientException e) {
			log.error("Shutdown session factory failed", e);
		}

	}

	@Override
	public void nextTuple() {
		try {
			final Message message = this.messageQueue.poll(
					WAIT_FOR_NEXT_MESSAGE, TimeUnit.MILLISECONDS);
			if (message == null) {
				return;
			}
			this.collector.emit(this.scheme.deserialize(message.getData()));
		} catch (final InterruptedException e) {
			log.error("There is something wrong in polling message from the queue"
					+ e);
		}
	}

	@Override
	public void declareOutputFields(final OutputFieldsDeclarer declarer) {
		declarer.declare(this.scheme.getOutputFields());
	}

}
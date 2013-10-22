package com.jd.metamorphosis.util;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.jd.metamorphosis.storm.spout.MetaqMessageWrapper;
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
 * @author darwin <caishize@outlook.com>
 * @date 2012-12-20
 * 
 */

public class MetaqReceiver {

	private transient MessageConsumer messageConsumer;

	private transient MessageSessionFactory sessionFactory;

	private final MetaClientConfig metaClientConfig;

	private final ConsumerConfig consumerConfig;

	static final Log log = LogFactory.getLog(MetaqReceiver.class);

	private transient ConcurrentHashMap<Long, MetaqMessageWrapper> id2wrapperMap;

	private transient LinkedTransferQueue<MetaqMessageWrapper> messageQueue;
	private boolean ack = false;

	public MetaqReceiver(@SuppressWarnings("rawtypes") final Map conf,
			final MetaClientConfig metaClientConfig,
			final ConsumerConfig consumerConfig,
			final LinkedTransferQueue<MetaqMessageWrapper> messageQueue) {
		this.metaClientConfig = metaClientConfig;
		this.consumerConfig = consumerConfig;
		this.messageQueue = messageQueue;
		this.id2wrapperMap = new ConcurrentHashMap<Long, MetaqMessageWrapper>();
		final Boolean ack = (Boolean) conf.get(Constants.ACK);
		if (ack != null) {
			this.ack = ack;
		}
		open(conf);
	}

	public void open(@SuppressWarnings("rawtypes") final Map conf) {
		final String topic = (String) conf.get(Constants.TOPIC);
		if (topic == null) {
			throw new IllegalArgumentException(Constants.TOPIC + " is null");
		}
		Long tmpMaxSize = (Long) conf.get(Constants.FETCH_MAX_SIZE);
		int maxSize;
		if (tmpMaxSize == null) {
			log.warn("Using default FETCH_MAX_SIZE");
			maxSize = Constants.DEFAULT_MAX_SIZE;
		} else {
			maxSize = Integer.parseInt(Long.toString(tmpMaxSize));
		}
		try {
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
				final MetaqMessageWrapper wrapper = new MetaqMessageWrapper(
						message);
				MetaqReceiver.this.messageQueue.offer(wrapper);
				if (MetaqReceiver.this.ack) {
					MetaqReceiver.this.id2wrapperMap.put(message.getId(),
							wrapper);
					try {
						wrapper.latch.await();
					} catch (final InterruptedException e) {
						Thread.currentThread().interrupt();
					}
					if (!wrapper.success) {
						throw new RuntimeException("Consume message failed");
					}
				}
			}

			public Executor getExecutor() {
				return null;
			}
		}).completeSubscribe();
	}

	public void ack(final long msgId) {
		if (ack) {
			final long id = (Long) msgId;
			final MetaqMessageWrapper wrapper = this.id2wrapperMap.remove(id);
			if (wrapper == null) {
				log.warn(String.format("don't know how to ack(%s)", msgId));
				return;
			}
			wrapper.success = true;
			wrapper.latch.countDown();
		}
	}

	public void fail(final long msgId) {
		if (ack) {
			final long id = (Long) msgId;
			final MetaqMessageWrapper wrapper = this.id2wrapperMap.remove(id);
			if (wrapper == null) {
				log.warn(String.format("don't know how to reject(%s)", msgId));
				return;
			}
			wrapper.success = false;
			wrapper.latch.countDown();
		}

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

}

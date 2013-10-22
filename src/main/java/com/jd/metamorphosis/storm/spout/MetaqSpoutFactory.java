package com.jd.metamorphosis.storm.spout;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.taobao.metamorphosis.client.MetaClientConfig;
import com.taobao.metamorphosis.client.consumer.ConsumerConfig;
import com.taobao.metamorphosis.utils.ZkUtils.ZKConfig;
/*
 * @author darwin<caishize@360buy.com>
 */
public class MetaqSpoutFactory {
	private static Logger LOG = Logger.getLogger(MetaqSpoutFactory.class);
	private static final String ZKURL = "zk.url";
	private static final String ZKROOT = "zk.root";
	private static final String GROUP = "metaq.consumer.group";
	private static final String TOPIC = "metaq.consumer.topic";
	private static final String STARTFROMEND = "ConsumeFromMaxOffset";
	public static String topic;

	public static MetaqSpout createSpout(String metaqConfigLocation) {
		InputStream is = null;
		MetaqSpout ms = null;
		try {
			is = MetaqSpoutFactory.class.getClassLoader().getResourceAsStream(
					metaqConfigLocation);
			Properties props = new Properties();
			props.load(is);
			String zkurl = props.getProperty(ZKURL);
			String zkroot = props.getProperty(ZKROOT);
			String group = props.getProperty(GROUP);
			MetaqSpoutFactory.topic = props.getProperty(TOPIC);
			Boolean startfromend = Boolean.valueOf(props
					.getProperty(STARTFROMEND));

			ConsumerConfig consumerConfig = new ConsumerConfig(group);
			if (startfromend) {
				consumerConfig.setConsumeFromMaxOffset();
			}
			final MetaClientConfig metaClientConfig = new MetaClientConfig();
			final ZKConfig zkConfig = new ZKConfig();
			zkConfig.zkConnect = zkurl;
			zkConfig.zkRoot = zkroot;
			metaClientConfig.setZkConfig(zkConfig);
			ms = new MetaqSpout(metaClientConfig, consumerConfig);

		} catch (Exception e) {
			LOG.error("Exception raised in creating metaq spout!");
			LOG.error(e);
		} finally {
			try {
				is.close();
			} catch (IOException e) {
				LOG.error("close inputstream fail!");
			}
		}
		return ms;
	}

}

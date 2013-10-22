package com.jd.metamorphosis.util;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/*
 * @author darwin <caishize@outlook.com> ;  @author xiaoguang
 * @date 2012-12-13
 */

public class MessageUtils {

	static final Log log = LogFactory.getLog(MessageUtils.class);

	/**
	 * @param data
	 * @return
	 */
	public static List<byte[]> readMessage(byte[] data) {

		List<byte[]> messageList = null;
		if (data != null && data.length > 4) {
			int index = 0;
			messageList = new ArrayList<byte[]>();
			try {
				while (index < data.length - 4) {
					ByteBuffer bb = ByteBuffer.wrap(data, index, 4);
					int len = bb.getInt();
					index += 4;
					if (len > Short.MAX_VALUE) {
						break;
					}
					if (index < data.length - len + 1) {
						messageList.add(Arrays.copyOfRange(data, index, index
								+ len));
					} else {
						log.error("There is something wrong with the data source：source length not enough");
						break;
					}
					index += len;
				}
			} catch (Exception e) {
				log.error(e);
			}

		}
		return messageList;
	}

}

package com.jd.metamorphosis.storm.scheme;

import java.util.Collections;
import java.util.List;

import com.jd.metamorphosis.util.MessageUtils;


import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;

public class ByteScheme implements Scheme {
	
	private static final long serialVersionUID = 18297428402262353L;

	@Override
	public List<Object> deserialize(byte[] ser) {
		return Collections.<Object> singletonList(MessageUtils.readMessage(ser));
	}
	@Override
	public Fields getOutputFields() {
		return new Fields("message");
	}

}

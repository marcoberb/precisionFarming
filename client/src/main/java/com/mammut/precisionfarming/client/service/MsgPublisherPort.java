package com.mammut.precisionfarming.client.service;

public interface MsgPublisherPort {

    void publishToServer(String channelSuffix, String payload);

}

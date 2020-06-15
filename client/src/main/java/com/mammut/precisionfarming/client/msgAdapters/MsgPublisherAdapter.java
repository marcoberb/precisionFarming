package com.mammut.precisionfarming.client.msgAdapters;

import com.mammut.precisionfarming.client.service.MsgPublisherPort;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class MsgPublisherAdapter implements MsgPublisherPort {

    @Autowired
    private KafkaTemplate<String, String> template; // intelliJ da errore ma in realtà funziona correttamente

    @Value("${kafka.channel.out}")
    private String channel;

    @Override
    public void publishToServer(String payload) {
        log.info("invio sul topic: " + this.channel + " il messaggio: " + payload);
        template.send(this.channel, payload);
    }
}

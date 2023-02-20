package com.khalid.kafka;

import com.khalid.kafka.entities.WikimediaData;
import com.khalid.kafka.repository.WikimediaDataRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaDataBaseConsumer {
    private WikimediaDataRepository wikimediaDataRepository;
    private static final Logger LOGGER= LoggerFactory.getLogger(KafkaDataBaseConsumer.class);

    public KafkaDataBaseConsumer(WikimediaDataRepository wikimediaDataRepository) {
        this.wikimediaDataRepository = wikimediaDataRepository;
    }

    @KafkaListener(topics = "wikimedia_recentchange",groupId = "myGroup")
    public void consume(String eventMessage){
        LOGGER.info(String.format("Event Message received -> %s",eventMessage));

        WikimediaData wikimediaData = new WikimediaData();
        wikimediaData.setWikiEventData(eventMessage);
        wikimediaDataRepository.save(wikimediaData);
    }
}

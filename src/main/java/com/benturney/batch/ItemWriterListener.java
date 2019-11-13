package com.benturney.batch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ItemWriteListener;

import java.util.List;

public class ItemWriterListener implements ItemWriteListener<Person> {
    private static final Logger log = LoggerFactory.getLogger(ItemWriterListener.class);

    @Override
    public void beforeWrite(List<? extends Person> items) {

    }

    @Override
    public void afterWrite(List<? extends Person> items) {
        log.info("Wrote {} records to file", items.size());
    }

    @Override
    public void onWriteError(Exception exception, List<? extends Person> items) {
        log.warn("Error writing {} records. Cause:\n{}", items.size(), exception);
    }
}

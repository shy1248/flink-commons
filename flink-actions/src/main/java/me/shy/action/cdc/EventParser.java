package me.shy.action.cdc;

import java.io.Serializable;
import java.util.List;
import me.shy.action.cdc.sink.iceberg.CdcRecord;

public interface EventParser <T> {

    /** Set current raw event to the parser. */
    void setRawEvent(T rawEvent);

    /** Parse the table name from raw event. */
    default String parseTableName() {
        throw new UnsupportedOperationException("Table name is not supported in this parser.");
    }

    /**
     * Parse records from event.
     *
     * @return empty if there is no records
     */
    List<CdcRecord> parseRecords();


    /** Factory to create an {@link EventParser}. */
    interface Factory<T> extends Serializable {

        EventParser<T> create();
    }
}

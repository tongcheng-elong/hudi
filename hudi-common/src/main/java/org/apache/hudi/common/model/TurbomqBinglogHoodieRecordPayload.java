package org.apache.hudi.common.model;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;

import java.io.IOException;
import java.util.Properties;

public class TurbomqBinglogHoodieRecordPayload extends DefaultHoodieRecordPayload {
    public TurbomqBinglogHoodieRecordPayload(GenericRecord record, Comparable orderingVal) {
        super(record, orderingVal);
    }

    public TurbomqBinglogHoodieRecordPayload(Option<GenericRecord> record) {
        super(record);
    }

    @Override
    public Option<IndexedRecord> getInsertValue(Schema schema, Properties properties) throws IOException {
        Option<IndexedRecord> record = super.getInsertValue(schema, properties);
        return record.isPresent() ? handleDeleteOperation(record.get()) : record;
    }

    @Override
    public Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Schema schema, Properties properties) throws IOException {
        Option<IndexedRecord> indexedRecordOption = super.combineAndGetUpdateValue(currentValue, schema, properties);
        return indexedRecordOption.isPresent() ? handleDeleteOperation(indexedRecordOption.get()) : indexedRecordOption;
    }

    @Override
    protected boolean needUpdatingPersistedRecord(IndexedRecord currentValue,
                                                  IndexedRecord incomingRecord, Properties properties) {
        /*
         * Combining strategy here returns currentValue on disk if incoming record is older.
         * The incoming record can be either a delete (sent as an upsert with _hoodie_is_deleted set to true)
         * or an insert/update record. In any case, if it is older than the record in disk, the currentValue
         * in disk is returned (to be rewritten with new commit time).
         *
         * NOTE: Deletes sent via EmptyHoodieRecordPayload and/or Delete operation type do not hit this code path
         * and need to be dealt with separately.
         */
        String orderField = properties.getProperty(HoodiePayloadProps.PAYLOAD_ORDERING_FIELD_PROP_KEY);
        if (orderField == null) {
            return true;
        }
        boolean consistentLogicalTimestampEnabled = Boolean.parseBoolean(properties.getProperty(
                KeyGeneratorOptions.KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED.key(),
                KeyGeneratorOptions.KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED.defaultValue()));
        Object persistedOrderingVal = HoodieAvroUtils.getNestedFieldVal((GenericRecord) currentValue,
                orderField,
                true, consistentLogicalTimestampEnabled);
        Comparable incomingOrderingVal = (Comparable) HoodieAvroUtils.getNestedFieldVal((GenericRecord) incomingRecord,
                orderField,
                true, consistentLogicalTimestampEnabled);
        return persistedOrderingVal == null || ((Comparable) persistedOrderingVal).toString().compareTo(incomingOrderingVal.toString()) <= 0;
    }

    private Option<IndexedRecord> handleDeleteOperation(IndexedRecord insertRecord) {
        boolean delete = false;
        if (insertRecord instanceof GenericRecord) {
            GenericRecord record = (GenericRecord) insertRecord;
            Object value = record.get("binlog_eventtype");
            delete = value != null && value.toString().equalsIgnoreCase("delete");
        }

        return delete ? Option.empty() : Option.of(insertRecord);
    }

}

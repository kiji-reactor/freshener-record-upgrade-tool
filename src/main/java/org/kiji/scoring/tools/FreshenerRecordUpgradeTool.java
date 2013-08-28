package org.kiji.scoring.tools;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import org.kiji.schema.Kiji;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiURI;
import org.kiji.schema.tools.BaseTool;
import org.kiji.scoring.KijiFreshnessManager;
import org.kiji.scoring.avro.KijiFreshenerRecord;
import org.kiji.scoring.avro.KijiFreshnessPolicyRecord;
import org.kiji.scoring.avro.ScorerRecord;

public class FreshenerRecordUpgradeTool extends BaseTool {

  // Positional argument for the table to upgrade.

  // Flags will be added here to select upgrades when more than one upgrade is available.


  @Override
  public String getName() {
    return "record-upgrade";
  }

  @Override
  public String getDescription() {
    return "upgrade existing Kiji-Scoring freshener records to be compatible with a newer version.";
  }

  @Override
  public String getCategory() {
    return "Metadata";
  }

  private KijiFreshnessPolicyRecord retrieveOldRecord(
      final String tableName,
      final String columnName,
      final Kiji kiji
  ) throws IOException {
    final byte[] recordBytes = kiji.getMetaTable().getValue(
        tableName, KijiFreshnessManager.METATABLE_KEY_PREFIX + columnName);
    final DecoderFactory factory = DecoderFactory.get();
    final Decoder decoder = factory.binaryDecoder(recordBytes, null);
    final DatumReader<KijiFreshnessPolicyRecord> recordReader =
        new SpecificDatumReader<KijiFreshnessPolicyRecord>(KijiFreshnessPolicyRecord.SCHEMA$);
    return recordReader.read(null, decoder);
  }

  private Map<KijiColumnName, KijiFreshnessPolicyRecord> retrieveOldRecords(
      final String tableName,
      final Kiji kiji
  ) throws IOException {
    final Set<String> keySet = kiji.getMetaTable().keySet(tableName);
    final Map<KijiColumnName, KijiFreshnessPolicyRecord> records = Maps.newHashMap();
    for (String key : keySet) {
      if (key.startsWith(KijiFreshnessManager.METATABLE_KEY_PREFIX)) {
        final String columnName = key.substring(KijiFreshnessManager.METATABLE_KEY_PREFIX.length());
        records.put(new KijiColumnName(columnName), retrieveOldRecord(tableName, columnName, kiji));
      }
    }
    return records;
  }

  private Map<KijiColumnName, KijiFreshenerRecord> convertRecords(
      final Map<KijiColumnName, KijiFreshnessPolicyRecord> oldRecords
  ) {
    final Map<KijiColumnName, KijiFreshenerRecord> newRecords = Maps.newHashMap();
    for (Map.Entry<KijiColumnName, KijiFreshnessPolicyRecord> entry : oldRecords.entrySet()) {
      final KijiFreshnessPolicyRecord oldRecord = entry.getValue();
      final KijiFreshenerRecord newRecord = KijiFreshenerRecord.newBuilder()
          .setRecordVersion("policyrecord-0.2")
          .setFreshnessPolicyClass(oldRecord.getFreshnessPolicyClass())
          .setFreshnessPolicyState(oldRecord.getFreshnessPolicyState())
          .setConfiguration(Collections.EMPTY_MAP)
          .setScorerRecord(ScorerRecord.newBuilder()
              .setScorerClass("org.kiji.scoring.impl.ProducerScorer")
              .setScorerIdentifier(oldRecord.getProducerClass())
              .build())
          .build();
      newRecords.put(entry.getKey(), newRecord);
    }
    return newRecords;
  }

  private void writeNewRecords(
      final Map<KijiColumnName, KijiFreshenerRecord> newRecords,
      final String tableName,
      final Kiji kiji
  ) throws IOException {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final EncoderFactory factory = EncoderFactory.get();
    final SpecificDatumWriter<KijiFreshenerRecord> recordWriter =
        new SpecificDatumWriter<KijiFreshenerRecord>(KijiFreshenerRecord.SCHEMA$);
    for (Map.Entry<KijiColumnName, KijiFreshenerRecord> entry : newRecords.entrySet()) {
      baos.reset();
      final Encoder encoder = factory.directBinaryEncoder(baos, null);
      recordWriter.write(entry.getValue(), encoder);
      kiji.getMetaTable().putValue(
          tableName,
          KijiFreshnessManager.METATABLE_KEY_PREFIX + entry.getKey().getName(),
          baos.toByteArray());
    }

  }

  @Override
  protected int run(final List<String> strings) throws Exception {
    final KijiURI uri = KijiURI.newBuilder(strings.get(0)).build();
    Preconditions.checkArgument(null != uri.getTable(),
        "Please specify a table to upgrade in your KijiURI");
    final Kiji kiji = Kiji.Factory.open(uri);
    try {
      final Map<KijiColumnName, KijiFreshnessPolicyRecord> oldRecords =
          retrieveOldRecords(uri.getTable(), kiji);
      final Map<KijiColumnName, KijiFreshenerRecord> newRecords = convertRecords(oldRecords);
      writeNewRecords(newRecords, uri.getTable(), kiji);
    } finally {
      kiji.release();
    }
    return BaseTool.SUCCESS;
  }
}

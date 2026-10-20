/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.ops.provider;

import org.apache.calcite.adapter.ops.CloudOpsConfig;
import org.apache.calcite.adapter.ops.StorageResourcesTable;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;

import com.google.cloud.compute.v1.CustomerEncryptionKey;
import com.google.cloud.compute.v1.Disk;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import software.amazon.awssdk.services.ec2.model.Volume;
import software.amazon.awssdk.services.ec2.model.VolumeAttachment;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Attached disks as storage_resources rows: an EBS volume and a persistent disk mapped from the
 * clouds' own model objects, with the key a customer-managed encryption uses and the VM the disk
 * is attached to. No cloud calls.
 */
@Tag("unit")
class StorageDiskRowsTest {

  private static final String CMK =
      "arn:aws:kms:us-east-1:111122223333:key/1234abcd-12ab-34cd-56ef-1234567890ab";
  private static final String AWS_MANAGED =
      "arn:aws:kms:us-east-1:111122223333:key/aaaa1111-12ab-34cd-56ef-1234567890ab";

  /** Who manages each key, as KMS DescribeKey answers; an unknown key cannot be read. */
  private static String manager(String keyId) {
    return CMK.equals(keyId) ? "CUSTOMER" : AWS_MANAGED.equals(keyId) ? "AWS" : null;
  }

  private static Volume.Builder volume() {
    return Volume.builder().volumeId("vol-0abc").size(8).volumeType("gp3")
        .tags(software.amazon.awssdk.services.ec2.model.Tag.builder()
            .key("Name").value("web-root").build());
  }

  @Test void storageResourcesEndsWithTheKeyAndTheAttachedInstance() {
    List<String> columns = new StorageResourcesTable(
        new CloudOpsConfig(null, null, null, null, null, null, null))
        .getRowType(new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT)).getFieldNames();
    assertEquals(Arrays.asList("tags", "key_id", "attached_instance_id"),
        columns.subList(columns.size() - 3, columns.size()));
  }

  @Test void anAttachedEbsVolumeNamesItsCustomerKeyAndItsInstance() {
    Map<String, Object> row = AWSProvider.ebsVolume("111122223333", "us-east-1",
        volume().encrypted(true).kmsKeyId(CMK)
            .attachments(VolumeAttachment.builder().instanceId("i-0123").build(),
                VolumeAttachment.builder().instanceId("i-0456").build())
            .build(),
        StorageDiskRowsTest::manager);
    assertEquals("EBS Volume", row.get("StorageType"));
    assertEquals("web-root", row.get("StorageResource"));
    assertEquals("arn:aws:ec2:us-east-1:111122223333:volume/vol-0abc", row.get("ResourceId"));
    // As compute_resources.resource_id names the instance; the first of a Multi-Attach volume
    assertEquals("arn:aws:ec2:us-east-1:111122223333:instance/i-0123",
        row.get("AttachedInstanceId"));
    assertEquals(8L * 1024 * 1024 * 1024, row.get("SizeBytes"));
    assertEquals("gp3", row.get("StorageClass"));
    assertEquals(true, row.get("EncryptionEnabled"));
    assertEquals(CMK, row.get("KeyId"));
    assertEquals("customer-managed", row.get("EncryptionKeyType"));
  }

  @Test void anEbsVolumeUnderAnAwsManagedKeyNamesNoKey() {
    Map<String, Object> row = AWSProvider.ebsVolume("111122223333", "us-east-1",
        volume().encrypted(true).kmsKeyId(AWS_MANAGED).build(), StorageDiskRowsTest::manager);
    assertNull(row.get("KeyId"));
    assertEquals("service-managed", row.get("EncryptionKeyType"));
    assertNull(row.get("AttachedInstanceId"));
  }

  @Test void aKeyWhoseManagerCannotBeReadIsNamedButNotClassified() {
    Map<String, Object> row = new HashMap<>();
    String unknown = "arn:aws:kms:us-east-1:111122223333:key/ffff";
    AWSProvider.putKey(row, unknown, StorageDiskRowsTest::manager);
    assertEquals(unknown, row.get("KeyId"));
    assertNull(row.get("EncryptionKeyType"));
  }

  @Test void anUnencryptedEbsVolumeHasNoKey() {
    Map<String, Object> row = AWSProvider.ebsVolume("111122223333", "us-east-1",
        volume().encrypted(false).build(), StorageDiskRowsTest::manager);
    assertEquals(false, row.get("EncryptionEnabled"));
    assertNull(row.get("KeyId"));
    assertNull(row.get("EncryptionKeyType"));
  }

  private static final String INSTANCE =
      "https://www.googleapis.com/compute/v1/projects/acme/zones/us-central1-a/instances/web-1";

  private static Disk.Builder disk() {
    return Disk.newBuilder().setName("web-1-boot").setSizeGb(10)
        .setZone("https://www.googleapis.com/compute/v1/projects/acme/zones/us-central1-a")
        .setType("https://www.googleapis.com/compute/v1/projects/acme/zones/us-central1-a/"
            + "diskTypes/pd-balanced")
        .setSelfLink("https://www.googleapis.com/compute/v1/projects/acme/zones/us-central1-a/"
            + "disks/web-1-boot")
        .setCreationTimestamp("2026-10-01T09:30:00.000-07:00");
  }

  @Test void anAttachedPersistentDiskNamesItsCloudKmsKeyAndItsInstance() {
    Map<String, Object> row = GCPProvider.persistentDisk("acme", disk()
        .setDiskEncryptionKey(CustomerEncryptionKey.newBuilder().setKmsKeyName(
            "projects/acme/locations/us/keyRings/ring/cryptoKeys/disks/cryptoKeyVersions/3"))
        .addUsers(INSTANCE)
        .build());
    assertEquals("Persistent Disk", row.get("StorageType"));
    assertEquals("us-central1-a", row.get("Location"));
    assertEquals(INSTANCE, row.get("AttachedInstanceId"));
    assertEquals(10L * 1024 * 1024 * 1024, row.get("SizeBytes"));
    assertEquals("pd-balanced", row.get("StorageClass"));
    // The key, not the version the disk records
    assertEquals("projects/acme/locations/us/keyRings/ring/cryptoKeys/disks",
        row.get("EncryptionKeyName"));
    assertEquals("customer-managed", row.get("EncryptionKeyType"));
  }

  @Test void aPersistentDiskUnderGooglesKeyNamesNoKey() {
    Map<String, Object> row = GCPProvider.persistentDisk("acme", disk().build());
    assertNull(row.get("EncryptionKeyName"));
    assertEquals("service-managed", row.get("EncryptionKeyType"));
    assertNull(row.get("AttachedInstanceId"));
  }

  @Test void aPersistentDiskUnderASuppliedKeyNamesNoKey() {
    Map<String, Object> row = GCPProvider.persistentDisk("acme", disk()
        .setDiskEncryptionKey(CustomerEncryptionKey.newBuilder().setSha256("abc")).build());
    assertNull(row.get("EncryptionKeyName"));
    assertEquals("customer-supplied", row.get("EncryptionKeyType"));
  }
}

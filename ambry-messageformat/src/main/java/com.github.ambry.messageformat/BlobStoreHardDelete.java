/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.github.ambry.messageformat;

import com.github.ambry.store.HardDeleteInfo;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.MessageReadSet;
import com.github.ambry.store.MessageStoreHardDelete;
import com.github.ambry.store.Read;
import com.github.ambry.store.StoreKey;
import com.github.ambry.store.StoreKeyFactory;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.ByteBufferOutputStream;
import com.github.ambry.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import static com.github.ambry.messageformat.MessageFormatRecord.*;


/**
 * This class takes a read set for blobs that are to be hard deleted and provides corresponding
 * replacement messages, that can then be written back by the caller to hard delete those blobs.
 */
public class BlobStoreHardDelete implements MessageStoreHardDelete {
    @Override
    public Iterator<HardDeleteInfo> getHardDeleteMessages(MessageReadSet readSet, StoreKeyFactory storeKeyFactory,
            List<byte[]> recoveryInfoList) throws IOException {
        return new BlobStoreHardDeleteIterator(readSet, storeKeyFactory, recoveryInfoList);
    }

    @Override
    public MessageInfo getMessageInfo(Read read, long offset, StoreKeyFactory storeKeyFactory) throws IOException {
        try {
            // read message header
            ByteBuffer headerVersion = ByteBuffer.allocate(Version_Field_Size_In_Bytes);
            read.readInto(headerVersion, offset);
            offset += headerVersion.capacity();
            headerVersion.flip();
            short version = headerVersion.getShort();
            MessageHeader_Format headerFormat;
            ReadInputStream stream;
            long endOffset;
            if (!isValidHeaderVersion(version)) {
                throw new MessageFormatException("Version not known while reading message - " + version,
                        MessageFormatErrorCodes.Unknown_Format_Version);
            }
            ByteBuffer header = ByteBuffer.allocate(getHeaderSizeForVersion(version));
            header.putShort(version);
            read.readInto(header, offset);
            offset += header.capacity() - headerVersion.capacity();
            header.flip();
            headerFormat = getMessageHeader(version, header);
            headerFormat.verifyHeader();
            endOffset = offset + headerFormat.getPayloadRelativeOffset() + headerFormat.getMessageSize();

      stream = new ReadInputStream(read, offset, endOffset);
      StoreKey key = storeKeyFactory.getStoreKey(new DataInputStream(stream));
      if (headerFormat.hasEncryptionKeyRecord()) {
        deserializeBlobEncryptionKey(stream);
      }
      // read the appropriate type of message based on the relative offset that is set
      if (headerFormat.isPutRecord()) {
        BlobProperties properties = deserializeBlobProperties(stream);
        return new MessageInfo(key, header.capacity() + key.sizeInBytes() + headerFormat.getMessageSize(),
            Utils.addSecondsToEpochTime(properties.getCreationTimeInMs(), properties.getTimeToLiveInSeconds()),
            properties.getAccountId(), properties.getContainerId(), properties.getCreationTimeInMs());
      } else {
        UpdateRecord updateRecord = deserializeUpdateRecord(stream);
        switch (updateRecord.getType()) {
          case DELETE:
            return new MessageInfo(key, header.capacity() + key.sizeInBytes() + headerFormat.getMessageSize(), true,
                false, updateRecord.getAccountId(), updateRecord.getContainerId(), updateRecord.getUpdateTimeInMs());
          case TTL_UPDATE:
            return new MessageInfo(key, header.capacity() + key.sizeInBytes() + headerFormat.getMessageSize(), false,
                true, updateRecord.getAccountId(), updateRecord.getContainerId(), updateRecord.getUpdateTimeInMs());
          default:
            throw new IllegalStateException("Unknown update record type: " + updateRecord.getType());
        }
      }
    } catch (MessageFormatException e) {
      // log in case where we were not able to parse a message.
      throw new IOException("Message format exception while parsing messages ", e);
    } catch (IndexOutOfBoundsException e) {
      // log in case where were not able to read a complete message.
      throw new IOException("Trying to read more than the available bytes");
    }
  }
}

class BlobStoreHardDeleteIterator implements Iterator<HardDeleteInfo> {
    private final MessageReadSet readSet;
    private final StoreKeyFactory storeKeyFactory;
    private Logger logger = LoggerFactory.getLogger(getClass());
    private int readSetIndex = 0;
    private Map<StoreKey, HardDeleteRecoveryMetadata> recoveryInfoMap;

    BlobStoreHardDeleteIterator(MessageReadSet readSet, StoreKeyFactory storeKeyFactory, List<byte[]> recoveryInfoList)
            throws IOException {
        this.readSet = readSet;
        this.storeKeyFactory = storeKeyFactory;
        this.recoveryInfoMap = new HashMap<StoreKey, HardDeleteRecoveryMetadata>();
        if (recoveryInfoList != null) {
            for (byte[] recoveryInfo : recoveryInfoList) {
                HardDeleteRecoveryMetadata hardDeleteRecoveryMetadata =
                        new HardDeleteRecoveryMetadata(recoveryInfo, storeKeyFactory);
                recoveryInfoMap.put(hardDeleteRecoveryMetadata.getStoreKey(), hardDeleteRecoveryMetadata);
            }
        }
    }

    @Override
    public boolean hasNext() {
        return readSetIndex != readSet.count();
    }

    @Override
    public HardDeleteInfo next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return getHardDeleteInfo(readSetIndex++);
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    /**
     * For the message at readSetIndex, does the following:
     1. Reads the whole blob and does a crc check. If the crc check fails, returns null - this means that the record
     is not retrievable anyway.
     2. Adds to a hard delete replacement write set.
     3. Returns the hard delete info.
     */
    private HardDeleteInfo getHardDeleteInfo(int readSetIndex) {

        HardDeleteInfo hardDeleteInfo = null;

        try {
      /* Read the version field in the header */
            ByteBuffer headerVersionBuf = ByteBuffer.allocate(Version_Field_Size_In_Bytes);
            readSet.writeTo(readSetIndex, Channels.newChannel(new ByteBufferOutputStream(headerVersionBuf)), 0,
                    Version_Field_Size_In_Bytes);
            headerVersionBuf.flip();
            short headerVersion = headerVersionBuf.getShort();
            if (!isValidHeaderVersion(headerVersion)) {
                throw new MessageFormatException(
                        "Unknown header version during hard delete " + headerVersion + " storeKey " + readSet.getKeyAt(
                                readSetIndex), MessageFormatErrorCodes.Unknown_Format_Version);
            }
            ByteBuffer header = ByteBuffer.allocate(getHeaderSizeForVersion(headerVersion));
      /* Read the rest of the header */
            header.putShort(headerVersion);
            readSet.writeTo(readSetIndex, Channels.newChannel(new ByteBufferOutputStream(header)),
                    Version_Field_Size_In_Bytes, header.capacity() - Version_Field_Size_In_Bytes);
            header.flip();
            MessageHeader_Format headerFormat = getMessageHeader(headerVersion, header);
            headerFormat.verifyHeader();
            StoreKey storeKey = storeKeyFactory.getStoreKey(
                    new DataInputStream(new MessageReadSetIndexInputStream(readSet, readSetIndex, header.capacity())));
            if (storeKey.compareTo(readSet.getKeyAt(readSetIndex)) != 0) {
                throw new MessageFormatException(
                        "Id mismatch between metadata and store - metadataId " + readSet.getKeyAt(readSetIndex) +
                                " storeId "
                                + storeKey, MessageFormatErrorCodes.Store_Key_Id_MisMatch);
            }

      if (!headerFormat.isPutRecord()) {
        throw new MessageFormatException("Cleanup operation for a non-PUT record is unsupported",
            MessageFormatErrorCodes.IO_Error);
      } else {
        if (headerFormat.hasEncryptionKeyRecord()) {
          ByteBuffer blobEncryptionKey =
              getBlobEncryptionKeyRecord(readSet, readSetIndex, headerFormat.getBlobEncryptionKeyRecordRelativeOffset(),
                  headerFormat.getBlobEncryptionKeyRecordSize());
        }
        BlobProperties blobProperties =
            getBlobPropertiesRecord(readSet, readSetIndex, headerFormat.getBlobPropertiesRecordRelativeOffset(),
                headerFormat.getBlobPropertiesRecordSize());

                HardDeleteRecoveryMetadata hardDeleteRecoveryMetadata = recoveryInfoMap.get(storeKey);

                int userMetadataRelativeOffset = headerFormat.getUserMetadataRecordRelativeOffset();
                short userMetadataVersion;
                int userMetadataSize;
                short blobRecordVersion;
                BlobType blobType;
                long blobStreamSize;
                DeserializedUserMetadata userMetadataInfo;
                DeserializedBlob blobRecordInfo;

                if (hardDeleteRecoveryMetadata == null) {
                    userMetadataInfo =
                            getUserMetadataInfo(readSet, readSetIndex,
                                    headerFormat.getUserMetadataRecordRelativeOffset(),
                                    headerFormat.getUserMetadataRecordSize());
                    userMetadataSize = userMetadataInfo.getUserMetadata().capacity();
                    userMetadataVersion = userMetadataInfo.getVersion();

                    blobRecordInfo = getBlobRecordInfo(readSet, readSetIndex,
                            headerFormat.getBlobRecordRelativeOffset(),
                            headerFormat.getBlobRecordSize());
                    blobStreamSize = blobRecordInfo.getBlobData().getSize();
                    blobRecordVersion = blobRecordInfo.getVersion();
                    blobType = blobRecordInfo.getBlobData().getBlobType();
                    hardDeleteRecoveryMetadata =
                            new HardDeleteRecoveryMetadata(headerVersion, userMetadataVersion, userMetadataSize,
                                    blobRecordVersion,
                                    blobType, blobStreamSize, storeKey);
                } else {
                    logger.trace("Skipping crc check for user metadata and blob stream fields for key {}", storeKey);
                    userMetadataVersion = hardDeleteRecoveryMetadata.getUserMetadataVersion();
                    blobRecordVersion = hardDeleteRecoveryMetadata.getBlobRecordVersion();
                    blobType = hardDeleteRecoveryMetadata.getBlobType();
                    userMetadataSize = hardDeleteRecoveryMetadata.getUserMetadataSize();
                    blobStreamSize = hardDeleteRecoveryMetadata.getBlobStreamSize();
                }

                HardDeleteMessageFormatInputStream hardDeleteStream =
                        new HardDeleteMessageFormatInputStream(userMetadataRelativeOffset, userMetadataVersion,
                                userMetadataSize,
                                blobRecordVersion, blobType, blobStreamSize);

                hardDeleteInfo = new HardDeleteInfo(Channels.newChannel(hardDeleteStream), hardDeleteStream.getSize(),
                        hardDeleteStream.getHardDeleteStreamRelativeOffset(), hardDeleteRecoveryMetadata.toBytes());
            }
        } catch (Exception e) {
            logger.error("Exception when reading blob: ", e);
        }
        return hardDeleteInfo;
    }

    /**
     * Get the Blob Encryption Key Record from the given readSet
     * @param readSet the {@link MessageReadSet} from which to read.
     * @param readSetIndex the index of the message in the readSet.
     * @param relativeOffset the relative offset in the message from which to read.
     * @param blobEncryptionKeySize the size of the record to read (in this case the encryption key record).
     * @return returns the read encryption key.
     * @throws MessageFormatException
     * @throws IOException
     */
    private ByteBuffer getBlobEncryptionKeyRecord(MessageReadSet readSet, int readSetIndex, long relativeOffset,
            long blobEncryptionKeySize) throws MessageFormatException, IOException {

    /* Read the field from the channel */
        ByteBuffer blobEncryptionKey = ByteBuffer.allocate((int) blobEncryptionKeySize);
        readSet.writeTo(readSetIndex, Channels.newChannel(new ByteBufferOutputStream(blobEncryptionKey)),
                relativeOffset,
                blobEncryptionKeySize);
        blobEncryptionKey.flip();

        return deserializeBlobEncryptionKey(new ByteBufferInputStream(blobEncryptionKey));
    }

    private BlobProperties getBlobPropertiesRecord(MessageReadSet readSet, int readSetIndex, long relativeOffset,
            long blobPropertiesSize) throws MessageFormatException, IOException {

    /* Read the field from the channel */
        ByteBuffer blobProperties = ByteBuffer.allocate((int) blobPropertiesSize);
        readSet.writeTo(readSetIndex, Channels.newChannel(new ByteBufferOutputStream(blobProperties)), relativeOffset,
                blobPropertiesSize);
        blobProperties.flip();

        return deserializeBlobProperties(new ByteBufferInputStream(blobProperties));
    }

    private DeserializedUserMetadata getUserMetadataInfo(MessageReadSet readSet, int readSetIndex, int relativeOffset,
            int userMetadataSize) throws MessageFormatException, IOException {

    /* Read the serialized user metadata from the channel */
        ByteBuffer userMetaData = ByteBuffer.allocate(userMetadataSize);
        readSet.writeTo(readSetIndex, Channels.newChannel(new ByteBufferOutputStream(userMetaData)), relativeOffset,
                userMetadataSize);
        userMetaData.flip();
        return deserializeAndGetUserMetadataWithVersion(new ByteBufferInputStream(userMetaData));
    }

    private DeserializedBlob getBlobRecordInfo(MessageReadSet readSet, int readSetIndex, int relativeOffset,
            long blobRecordSize) throws MessageFormatException, IOException {

    /* Read the field from the channel */
        ByteBuffer blobRecord = ByteBuffer.allocate((int) blobRecordSize);
        readSet.writeTo(readSetIndex, Channels.newChannel(new ByteBufferOutputStream(blobRecord)), relativeOffset,
                blobRecordSize);
        blobRecord.flip();
        return deserializeAndGetBlobWithVersion(new ByteBufferInputStream(blobRecord));
    }
}

class HardDeleteRecoveryMetadata {
    private short headerVersion;
    private short userMetadataVersion;
    private int userMetadataSize;
    private short blobRecordVersion;
    private BlobType blobType;
    private long blobStreamSize;
    private StoreKey storeKey;

    HardDeleteRecoveryMetadata(short headerVersion, short userMetadataVersion, int userMetadataSize,
            short blobRecordVersion, BlobType blobType, long blobStreamSize, StoreKey storeKey) throws IOException {
        if (!isValidHeaderVersion(headerVersion) || !isValidUserMetadataVersion(userMetadataVersion)
                || !isValidBlobRecordVersion(blobRecordVersion)) {
            throw new IOException(
                    "Unknown version during hard delete, headerVersion: " + headerVersion + " userMetadataVersion: "
                            + userMetadataVersion + " blobRecordVersion: " + blobRecordVersion);
        }
        this.headerVersion = headerVersion;
        this.userMetadataVersion = userMetadataVersion;
        this.userMetadataSize = userMetadataSize;
        this.blobRecordVersion = blobRecordVersion;
        this.blobType = blobType;
        this.blobStreamSize = blobStreamSize;
        this.storeKey = storeKey;
    }

    HardDeleteRecoveryMetadata(byte[] hardDeleteRecoveryMetadataBytes, StoreKeyFactory factory) throws IOException {
        DataInputStream stream = new DataInputStream(new ByteArrayInputStream(hardDeleteRecoveryMetadataBytes));
        headerVersion = stream.readShort();
        userMetadataVersion = stream.readShort();
        userMetadataSize = stream.readInt();
        blobRecordVersion = stream.readShort();
        if (blobRecordVersion == Blob_Version_V2) {
            blobType = BlobType.values()[stream.readShort()];
        } else {
            blobType = BlobType.DataBlob;
        }
        blobStreamSize = stream.readLong();
        if (!isValidHeaderVersion(headerVersion) || !isValidUserMetadataVersion(userMetadataVersion)
                || !isValidBlobRecordVersion(blobRecordVersion)) {
            throw new IOException(
                    "Unknown version during hard delete, headerVersion: " + headerVersion + " userMetadataVersion: "
                            + userMetadataVersion + " blobRecordVersion: " + blobRecordVersion + " blobType " +
                            blobType);
        }
        storeKey = factory.getStoreKey(stream);
    }

    StoreKey getStoreKey() {
        return storeKey;
    }

    short getHeaderVersion() {
        return headerVersion;
    }

    short getUserMetadataVersion() {
        return userMetadataVersion;
    }

    int getUserMetadataSize() {
        return userMetadataSize;
    }

    short getBlobRecordVersion() {
        return blobRecordVersion;
    }

    BlobType getBlobType() {
        return blobType;
    }

    long getBlobStreamSize() {
        return blobStreamSize;
    }

    byte[] toBytes() {
        // create a byte array to hold the headerVersion + userMetadataVersion + userMetadataSize + blobRecordVersion +
        // blobType + blobRecordSize + storeKey.
        byte[] bytes = new byte[Version_Field_Size_In_Bytes + Version_Field_Size_In_Bytes + Integer.SIZE / 8
                + Version_Field_Size_In_Bytes + (blobRecordVersion == Blob_Version_V2 ? (Short.SIZE / 8) : 0) +
                Long.SIZE / 8
                + storeKey.sizeInBytes()];

        ByteBuffer bufWrap = ByteBuffer.wrap(bytes);
        bufWrap.putShort(headerVersion);
        bufWrap.putShort(userMetadataVersion);
        bufWrap.putInt(userMetadataSize);
        bufWrap.putShort(blobRecordVersion);
        if (blobRecordVersion == Blob_Version_V2) {
            bufWrap.putShort((short) blobType.ordinal());
        }
        bufWrap.putLong(blobStreamSize);
        bufWrap.put(storeKey.toBytes());
        return bytes;
    }
}

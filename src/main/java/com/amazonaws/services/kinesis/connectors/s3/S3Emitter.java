/*
 * Copyright 2013-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.amazonaws.services.kinesis.connectors.s3;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration;
import com.amazonaws.services.kinesis.connectors.UnmodifiableBuffer;
import com.amazonaws.services.kinesis.connectors.interfaces.IEmitter;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;

/**
 * This implementation of IEmitter is used to store files from an Amazon Kinesis stream in S3. The use of
 * this class requires the configuration of an Amazon S3 bucket/endpoint. When the buffer is full, this
 * class's emit method adds the contents of the buffer to Amazon S3 as one file. The filename is generated
 * from the first and last sequence numbers of the records contained in that file separated by a
 * dash. This class requires the configuration of an Amazon S3 bucket and endpoint.
 *
 * This implementation isn't great on memory usage, so be warned if you are trying to emit
 * a large amount data per file, you'll need at least 3x the data size memory allocated.
 */
public class S3Emitter implements IEmitter<byte[]>, FilenameStrategy {
    private static final Log LOG = LogFactory.getLog(S3Emitter.class);
    protected final String s3Bucket;
    protected final String s3Endpoint;

    protected final AmazonS3Client s3client;

    protected String outputPrefix;
    protected FilenameStrategy filenameStrategy;

    public S3Emitter(KinesisConnectorConfiguration configuration) {
        s3Bucket = configuration.S3_BUCKET;
        s3Endpoint = configuration.S3_ENDPOINT;
        s3client = new AmazonS3Client(configuration.AWS_CREDENTIALS_PROVIDER);
        if (s3Endpoint != null) {
            s3client.setEndpoint(s3Endpoint);
        }
    }

    protected String getS3URI(String s3FileName) {
        return "s3://" + s3Bucket + "/" + s3FileName;
    }

    public String getFilename(UnmodifiableBuffer<byte[]> buffer) {
        String s3FileName = filenameStrategy.getFilename(buffer);
        if (outputPrefix != null) {
            s3FileName = outputPrefix + s3FileName;
        }
        return s3FileName;
    }

    @Override
    public List<byte[]> emit(final UnmodifiableBuffer<byte[]> buffer) throws IOException {
        List<byte[]> records = buffer.getRecords();
        byte[] outputBytes = null;
        try {
            outputBytes = transformRecordsToBytes(records);
        } catch (RecordProcessingException e) {
            LOG.error("Error writing record to output stream. Failing this emit attempt. Record: "
                    + Arrays.toString(e.record),
                    e);
            return records;
        } catch (IOException e) {
            LOG.error("Unexpected error transforming records", e);
            return records;
        }

        // Get the Amazon S3 filename
        String s3FileName = getFilename(buffer);

        String s3URI = getS3URI(s3FileName);
        try {
            LOG.debug("Starting upload of file " + s3URI + " to Amazon S3 containing " + records.size() + " records.");
            uploadByteArray(outputBytes, s3FileName);
            LOG.info("Successfully emitted " + buffer.getRecords().size() + " records to Amazon S3 in " + s3URI);
            return Collections.emptyList();
        } catch (Exception e) {
            LOG.error("Caught exception when uploading file " + s3URI + "to Amazon S3. Failing this emit attempt.", e);
            return buffer.getRecords();
        }
    }

    /**
     * Responsible for transforming the input records to a byte[] that will be written
     * to s3.  The default implementation writes the input records as-is.  Subclasses
     * can override this to compress the records.
     *
     * @param records
     * @return
     * @throws IOException
     */
    protected byte[] transformRecordsToBytes(List<byte[]> records) throws IOException {
        // Write all of the records to a compressed output stream
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        for (byte[] record : records) {
            baos.write(record);
        }
        return baos.toByteArray();
    }

    /**
     * Uploads the specified byte array to s3.
     *
     * @param bytes
     * @param s3Key
     * @throws IOException
     */
    protected void uploadByteArray(byte[] bytes, String s3Key) throws IOException {
        ByteArrayInputStream object = new ByteArrayInputStream(bytes);
        ObjectMetadata meta = new ObjectMetadata();
        meta.setContentLength(bytes.length);
        s3client.putObject(s3Bucket, s3Key, object, meta);
    }

    /**
     * Allows for a custom file naming strategy.  The default filenames may not be very useful,
     * this allows you to replace that a custom strategy, for example by a timestamp.
     *
     * @param strategy
     * @return
     */
    public S3Emitter withFilenameStrategy(FilenameStrategy strategy) {
        this.filenameStrategy = strategy;
        return this;
    }

    /**
     * All filenames will be prefixed with this path.  This allows you to specify the
     * directory hierarchy of the output instead of writing the data to the root
     * of the bucket.
     *
     * For example, a prefix of "logs/" will ensure all data written will in the
     * logs directory of the bucket.
     *
     * @param prefix
     * @return
     */
    public S3Emitter withOutputPrefix(String prefix) {
        this.outputPrefix = prefix;
        return this;
    }

    @Override
    public void fail(List<byte[]> records) {
        for (byte[] record : records) {
            LOG.error("Record failed: " + Arrays.toString(record));
        }
    }

    @Override
    public void shutdown() {
        s3client.shutdown();
    }


    public static class RecordProcessingException extends IOException {
        private final byte[] record;

        public RecordProcessingException(byte[] record) {
            this.record = record;
        }
    }

}

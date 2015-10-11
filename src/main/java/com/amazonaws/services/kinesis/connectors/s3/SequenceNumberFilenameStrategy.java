package com.amazonaws.services.kinesis.connectors.s3;

import com.amazonaws.services.kinesis.connectors.UnmodifiableBuffer;

/**
 * Names the files based on the first and last sequence number of the buffer.
 */
public class SequenceNumberFilenameStrategy implements FilenameStrategy {

    @Override
    public String getFilename(UnmodifiableBuffer<byte[]> buffer) {
        return buffer.getFirstSequenceNumber() + "-" + buffer.getLastSequenceNumber();
    }
}

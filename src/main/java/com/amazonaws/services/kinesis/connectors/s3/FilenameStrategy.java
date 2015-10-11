package com.amazonaws.services.kinesis.connectors.s3;

import com.amazonaws.services.kinesis.connectors.UnmodifiableBuffer;

/**
 * Responsible for choosing a filename.
 */
public interface FilenameStrategy {

    String getFilename(UnmodifiableBuffer<byte[]> buffer);
}

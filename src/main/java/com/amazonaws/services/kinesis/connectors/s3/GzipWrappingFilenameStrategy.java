package com.amazonaws.services.kinesis.connectors.s3;

import com.amazonaws.services.kinesis.connectors.UnmodifiableBuffer;

/**
 * Wraps another strategy, adding the .gz suffix if missing.
 */
public class GzipWrappingFilenameStrategy implements FilenameStrategy {

    private final FilenameStrategy original;

    public GzipWrappingFilenameStrategy(FilenameStrategy original) {
        this.original = original;
    }

    @Override
    public String getFilename(UnmodifiableBuffer<byte[]> buffer) {
        String filename = original.getFilename(buffer);
        if (!filename.endsWith(".gz")) {
            filename += ".gz";
        }
        return filename;
    }
}

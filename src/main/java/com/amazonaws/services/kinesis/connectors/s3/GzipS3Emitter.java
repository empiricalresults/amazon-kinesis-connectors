package com.amazonaws.services.kinesis.connectors.s3;


import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.zip.GZIPOutputStream;

/**
 * S3 emitter that emits gzipped content.
 */
public class GzipS3Emitter extends S3Emitter {

    private static final Log LOG = LogFactory.getLog(GzipS3Emitter.class);

    public GzipS3Emitter(KinesisConnectorConfiguration configuration) {
        super(configuration);
        this.filenameStrategy = new GzipWrappingFilenameStrategy(this.filenameStrategy);
    }

    @Override
    protected byte[] transformRecordsToBytes(List<byte[]> records) throws IOException {
        try (
                ByteArrayOutputStream baos = new ByteArrayOutputStream()
        ) {
            // separate resource block needed for the gzos, it doesn't properly flush until closed:
            // http://stackoverflow.com/q/3640080/424415
            try (GZIPOutputStream gzos = new GZIPOutputStream(baos)) {
                for (byte[] record : records) {
                    gzos.write(record);
                }
            }
            return baos.toByteArray();
        }
    }

    @Override
    public GzipS3Emitter withFilenameStrategy(FilenameStrategy strategy) {
        // not sure we should always wrap it this
        super.withFilenameStrategy(new GzipWrappingFilenameStrategy(strategy));
        return this;
    }

    @Override
    public GzipS3Emitter withOutputPrefix(String prefix) {
        super.withOutputPrefix(prefix);
        return this;
    }
}

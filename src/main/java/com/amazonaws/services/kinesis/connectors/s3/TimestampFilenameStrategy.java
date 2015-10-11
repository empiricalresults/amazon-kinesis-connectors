package com.amazonaws.services.kinesis.connectors.s3;


import com.amazonaws.services.kinesis.connectors.UnmodifiableBuffer;
import org.apache.commons.codec.digest.DigestUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * Creates filenames based on the current time.
 */
public class TimestampFilenameStrategy implements FilenameStrategy {

    private static final DateTimeFormatter FILENAME_FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd-HH.mm.ss");

    @Override
    public String getFilename(UnmodifiableBuffer<byte[]> buffer) {

        String datePart = FILENAME_FORMATTER.print(new DateTime(DateTimeZone.UTC));
        String digest = DigestUtils.md5Hex(buffer.getFirstSequenceNumber() + buffer.getLastSequenceNumber());

        StringBuilder sb = new StringBuilder();
        sb.append(datePart);
        sb.append("-");
        sb.append(digest);
        sb.append(".gz");

        return sb.toString();
    }
}

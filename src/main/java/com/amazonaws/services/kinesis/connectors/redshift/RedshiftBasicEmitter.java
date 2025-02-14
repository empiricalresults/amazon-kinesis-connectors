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
package com.amazonaws.services.kinesis.connectors.redshift;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration;
import com.amazonaws.services.kinesis.connectors.UnmodifiableBuffer;
import com.amazonaws.services.kinesis.connectors.s3.S3Emitter;

/**
 * This class is an implementation of IEmitter that emits records into Amazon Redshift one by one. It
 * utilizes the Amazon Redshift copy command on each file by first inserting records into Amazon S3 and then
 * performing the Amazon Redshift copy command. Amazon S3 insertion is done by extending the Amazon S3 emitter.
 * <p>
 * * This class requires the configuration of an Amazon S3 bucket and endpoint, as well as the following Amazon Redshift
 * items:
 * <ul>
 * <li>Redshift URL</li>
 * <li>username and password</li>
 * <li>data table and key column (data table stores items from the manifest copy)</li>
 * <li>file table and key column (file table is used to store file names to prevent duplicate entries)</li>
 * <li>the delimiter used for string parsing when inserting entries into Redshift</li>
 * <br>
 * NOTE: The Amazon S3 bucket and the Amazon Redshift cluster need to be in the same region.
 */
public class RedshiftBasicEmitter extends S3Emitter {
    private static final Log LOG = LogFactory.getLog(RedshiftBasicEmitter.class);
    private final String s3bucket;
    private final String redshiftTable;
    private final String redshiftURL;
    private final char redshiftDelimiter;
    private final Properties loginProperties;
    private final String accessKey;
    private final String secretKey;

    public RedshiftBasicEmitter(KinesisConnectorConfiguration configuration) {
        super(configuration);
        s3bucket = configuration.S3_BUCKET;
        redshiftTable = configuration.REDSHIFT_DATA_TABLE;
        redshiftDelimiter = configuration.REDSHIFT_DATA_DELIMITER;
        redshiftURL = configuration.REDSHIFT_URL;
        loginProperties = new Properties();
        loginProperties.setProperty("user", configuration.REDSHIFT_USERNAME);
        loginProperties.setProperty("password", configuration.REDSHIFT_PASSWORD);
        accessKey = configuration.AWS_CREDENTIALS_PROVIDER.getCredentials().getAWSAccessKeyId();
        secretKey = configuration.AWS_CREDENTIALS_PROVIDER.getCredentials().getAWSSecretKey();
    }

    @Override
    public List<byte[]> emit(final UnmodifiableBuffer<byte[]> buffer) throws IOException {
        List<byte[]> failed = super.emit(buffer);
        if (!failed.isEmpty()) {
            return buffer.getRecords();
        }
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(redshiftURL, loginProperties);
            String s3File = getFilename(buffer);
            executeStatement(generateCopyStatement(s3File), conn);
            LOG.info("Successfully copied " + getNumberOfCopiedRecords(conn)
                    + " records to Amazon Redshift from file s3://" + s3Bucket + "/" + s3File);
            return Collections.emptyList();
        } catch (Exception e) {
            LOG.error(e);
            return buffer.getRecords();
        } finally {
            closeConnection(conn);
        }
    }

    @Override
    public void fail(List<byte[]> records) {
        super.fail(records);
    }

    @Override
    public void shutdown() {
        super.shutdown();
    }

    private void closeConnection(Connection conn) {
        try {
            if (conn != null && !conn.isClosed()) {
                conn.close();
            }
        } catch (Exception e) {
            LOG.error(e);
        }
    }

    protected String generateCopyStatement(String s3File) {
        StringBuilder exec = new StringBuilder();
        exec.append("COPY " + redshiftTable + " ");
        exec.append("FROM 's3://" + s3bucket + "/" + s3File + "' ");
        exec.append("CREDENTIALS 'aws_access_key_id=" + accessKey);
        exec.append(";aws_secret_access_key=" + secretKey + "' ");
        exec.append("DELIMITER '" + redshiftDelimiter + "'");
        exec.append(";");
        return exec.toString();
    }

    private void executeStatement(String statement, Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(statement);
        }
    }

    private int getNumberOfCopiedRecords(Connection conn) throws SQLException {
        String cmd = "select pg_last_copy_count();";
        try (Statement stmt = conn.createStatement(); ResultSet resultSet = stmt.executeQuery(cmd)) {
            resultSet.next();
            int numCopiedRecords = resultSet.getInt(1);
            return numCopiedRecords;
        }
    }
}

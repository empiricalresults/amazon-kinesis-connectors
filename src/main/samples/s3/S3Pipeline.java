/*
 * Copyright 2013-2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
package samples.s3;

import com.amazonaws.services.kinesis.connectors.s3.GzipS3Emitter;
import com.amazonaws.services.kinesis.connectors.s3.TimestampFilenameStrategy;
import samples.KinesisMessageModel;

import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration;
import com.amazonaws.services.kinesis.connectors.impl.AllPassFilter;
import com.amazonaws.services.kinesis.connectors.impl.BasicMemoryBuffer;
import com.amazonaws.services.kinesis.connectors.impl.JsonToByteArrayTransformer;
import com.amazonaws.services.kinesis.connectors.interfaces.IBuffer;
import com.amazonaws.services.kinesis.connectors.interfaces.IEmitter;
import com.amazonaws.services.kinesis.connectors.interfaces.IFilter;
import com.amazonaws.services.kinesis.connectors.interfaces.IKinesisConnectorPipeline;
import com.amazonaws.services.kinesis.connectors.interfaces.ITransformer;
import com.amazonaws.services.kinesis.connectors.s3.S3Emitter;

/**
 * The Pipeline used by the Amazon S3 sample. Processes KinesisMessageModel records in JSON String
 * format. Uses:
 * <ul>
 * <li>S3Emitter</li>
 * <li>BasicMemoryBuffer</li>
 * <li>BasicJsonTransformer</li>
 * <li>AllPassFilter</li>
 * </ul>
 */
public class S3Pipeline implements IKinesisConnectorPipeline<KinesisMessageModel, byte[]> {

    @Override
    public IEmitter<byte[]> getEmitter(KinesisConnectorConfiguration configuration) {

        S3Emitter emitter = new S3Emitter(configuration);

        // gzip the output instead of raw data
        // S3Emitter emitter = new GzipS3Emitter(configuration);

        // if you want files to go to a specific directory in your bucket
        // emitter.withOutputPrefix("logs/raw/")

        // and finally, if you want better filenames, use a custom filename
        // strategy
        // emitter.withFilenameStrategy(new TimestampFilenameStrategy());

        return emitter;
    }

    @Override
    public IBuffer<KinesisMessageModel> getBuffer(KinesisConnectorConfiguration configuration) {
        return new BasicMemoryBuffer<KinesisMessageModel>(configuration);
    }

    @Override
    public ITransformer<KinesisMessageModel, byte[]> getTransformer(KinesisConnectorConfiguration configuration) {
        return new JsonToByteArrayTransformer<KinesisMessageModel>(KinesisMessageModel.class);
    }

    @Override
    public IFilter<KinesisMessageModel> getFilter(KinesisConnectorConfiguration configuration) {
        return new AllPassFilter<KinesisMessageModel>();
    }

}

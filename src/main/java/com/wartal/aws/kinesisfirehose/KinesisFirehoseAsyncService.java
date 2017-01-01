package com.wartal.aws.kinesisfirehose;

import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseAsyncClient;
import com.amazonaws.services.kinesisfirehose.model.PutRecordRequest;
import com.amazonaws.services.kinesisfirehose.model.PutRecordResult;
import com.amazonaws.services.kinesisfirehose.model.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Future;

/**
 * Created by lwartalski on 01/01/2017.
 */
@Service
public class KinesisFirehoseAsyncService implements KinesisFirehoseService {
    private Logger log = LoggerFactory.getLogger(getClass());

    @Autowired
    private AmazonKinesisFirehoseAsyncClient firehoseClient;

    @Override
    public Future<PutRecordResult> putRecord(String json, String deliveryStream) {
        Record record = new Record();
        record.setData(ByteBuffer.wrap(json.getBytes(StandardCharsets.UTF_8)));

        PutRecordRequest putRecordRequest = new PutRecordRequest()
                .withDeliveryStreamName(deliveryStream)
                .withRecord(record);

        return firehoseClient.putRecordAsync(putRecordRequest, asyncHandler());
    }

    private AsyncHandler<PutRecordRequest, PutRecordResult> asyncHandler() {
        return new AsyncHandler<PutRecordRequest, PutRecordResult>() {
            @Override
            public void onError(Exception e) {
                log.warn("Exception async handler {}", e);
            }

            @Override
            public void onSuccess(PutRecordRequest request, PutRecordResult putRecordResult) {
                log.info("PutRecordRequest {} PutRecordResult {}", request, putRecordResult);
            }
        };
    }
}

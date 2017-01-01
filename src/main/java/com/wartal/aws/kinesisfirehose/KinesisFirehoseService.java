package com.wartal.aws.kinesisfirehose;

import com.amazonaws.services.kinesisfirehose.model.PutRecordResult;

import java.util.concurrent.Future;

/**
 * Created by lwartalski on 01/01/2017.
 */
public interface KinesisFirehoseService {

    /**
     * Put record
     *
     * @param json           record request body
     * @param deliveryStream delivery stream name
     * @return
     */
    Future<PutRecordResult> putRecord(String json, String deliveryStream);
}

package com.wartal.aws.kinesisfirehose;

import com.amazonaws.services.kinesisfirehose.model.PutRecordResult;
import com.amazonaws.util.IOUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by lwartalski on 01/01/2017.
 */
@RunWith(SpringRunner.class)
@SpringBootTest(
        classes = {KinesisFirehoseAsyncService.class, KinesisFirehoseConfig.class})
public class KinesisFirehoseAsyncServiceIT {

    @Autowired
    private KinesisFirehoseAsyncService service;

    @Test
    public void shouldPutRecordWhenDeliveryStreamNameIsValid() throws Exception {
        final String deliveryStream = "valid-stream-name";

        final Future<PutRecordResult> future = service.putRecord(getJson(), deliveryStream);
        PutRecordResult record = future.get();

        assertThat(record).hasNoNullFieldsOrProperties();
        assertThat(record.getSdkHttpMetadata().getHttpStatusCode()).isEqualTo(200);
    }

    @Test(expected = ExecutionException.class)
    public void shouldThrowExceptionWhenDeliveryStreamIsInvalid() throws Exception {
        final String deliveryStream = "invalid-stream-name";
        final Future<PutRecordResult> future = service.putRecord(getJson(), deliveryStream);
        future.get();
    }

    private String getJson() throws IOException {
        return IOUtils.toString(getClass().getClassLoader().getResourceAsStream("test.json"));
    }
}
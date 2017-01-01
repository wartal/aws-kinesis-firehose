package com.wartal.aws.kinesisfirehose;

import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseAsyncClient;
import com.amazonaws.services.kinesisfirehose.model.PutRecordRequest;
import com.amazonaws.services.kinesisfirehose.model.PutRecordResult;
import com.amazonaws.util.IOUtils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.*;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Matchers.any;

/**
 * Created by lwartalski on 01/01/2017.
 */
public class KinesisFirehoseAsyncServiceTest {

    @InjectMocks
    private KinesisFirehoseAsyncService kinesisFirehoseAsyncService;

    @Mock
    private AmazonKinesisFirehoseAsyncClient client;

    @Captor
    ArgumentCaptor<PutRecordRequest> recordResultArgumentCaptor;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void shouldPutRecord() throws IOException, ExecutionException, InterruptedException {
        String id = "myId";
        String json = getJson();

        Future<PutRecordResult> future = Mockito.mock(Future.class);

        given(future.get()).willReturn(getPutRecordResult(id));
        given(client.putRecordAsync(Mockito.any(), Mockito.any())).willReturn(future);

        Future<PutRecordResult> pushResult = kinesisFirehoseAsyncService.putRecord(json, "testStream");
        then(client).should().putRecordAsync(recordResultArgumentCaptor.capture(), any());

        final PutRecordRequest putRecordRequest = recordResultArgumentCaptor.getValue();

        assertThat(putRecordRequest).isNotNull();
        assertThat(json.getBytes()).isEqualTo(putRecordRequest.getRecord().getData().array());
        assertThat(putRecordRequest.getDeliveryStreamName()).isEqualTo("testStream");
        assertThat(pushResult.get().getRecordId()).isEqualTo(id);
    }

    @Test
    public void shouldPutEmptyRecord() throws IOException, ExecutionException, InterruptedException {
        String id = "myId";

        Future<PutRecordResult> future = Mockito.mock(Future.class);

        given(future.get()).willReturn(getPutRecordResult(id));
        given(client.putRecordAsync(Mockito.any(), Mockito.any())).willReturn(future);

        Future<PutRecordResult> pushResult = kinesisFirehoseAsyncService.putRecord("", "testStream");
        then(client).should().putRecordAsync(recordResultArgumentCaptor.capture(), any());

        final PutRecordRequest putRecordRequest = recordResultArgumentCaptor.getValue();

        assertThat(putRecordRequest).isNotNull();
        assertThat("".getBytes()).isEqualTo(putRecordRequest.getRecord().getData().array());
        assertThat(putRecordRequest.getDeliveryStreamName()).isEqualTo("testStream");
        assertThat(pushResult.get().getRecordId()).isEqualTo(id);
    }

    private String getJson() throws IOException {
        return IOUtils.toString(getClass().getClassLoader().getResourceAsStream("test.json"));
    }

    private PutRecordResult getPutRecordResult(String id) {
        PutRecordResult result = new PutRecordResult();
        result.setRecordId(id);
        return result;
    }
}
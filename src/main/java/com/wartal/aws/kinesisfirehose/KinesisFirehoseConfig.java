package com.wartal.aws.kinesisfirehose;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseAsyncClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Created by lwartalski on 01/01/2017.
 */
@Configuration
public class KinesisFirehoseConfig {
    @Bean
    public AmazonKinesisFirehoseAsyncClient amazonKinesisFirehoseAsyncClient(
            @Value(value = "${firehose.endpoint}") String endpoint,
            @Value(value = "${firehose.accessKey}") String accessKey,
            @Value(value = "${firehose.secretKey}") String secretKey) {
        AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
        AmazonKinesisFirehoseAsyncClient firehoseClient = new AmazonKinesisFirehoseAsyncClient(credentials);
        firehoseClient.setEndpoint(endpoint);
        return firehoseClient;
    }
}

package com.wartal.aws.kinesisfirehose;

import com.amazonaws.util.IOUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;

import java.io.IOException;

import static org.mockito.BDDMockito.then;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Created by lwartalski on 01/01/2017.
 */
@RunWith(SpringRunner.class)
@WebMvcTest(controllers = Controller.class)
public class ControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private KinesisFirehoseAsyncService service;

    @Test
    public void shouldPutRecordRequestWithDeliveryStreamNameEqPlaytime() throws Exception {
        String json = getJson();

        this.mockMvc
                .perform(post("/playtime")
                        .content(json)
                        .contentType(MediaType.APPLICATION_JSON_VALUE))
                .andExpect(status().isOk());

        then(service).should().putRecord(json, "playtime");
    }

    @Test
    public void shouldPutRecordRequestWithRandomDeliveryStreamName() throws Exception {
        String json = getJson();

        this.mockMvc
                .perform(post("/04e4a8b9-b173-4f66-aef6-62f5b913de02")
                        .content(json)
                        .contentType(MediaType.APPLICATION_JSON_VALUE))
                .andExpect(status().isOk());

        then(service).should().putRecord(json, "04e4a8b9-b173-4f66-aef6-62f5b913de02");
    }

    @Test
    public void shouldReturnBadRequestWhenRequestBodyIsEmpty() throws Exception {
        this.mockMvc
                .perform(post("/playtime")
                        .contentType(MediaType.APPLICATION_JSON_VALUE))
                .andExpect(status().isBadRequest());

        verifyNoMoreInteractions(service);
    }

    @Test
    public void shouldReturnInternalServerErrorWhenServiceThrowException() throws Exception {
        String json = getJson();
        doThrow(Exception.class).when(service).putRecord(anyString(), anyString());

        this.mockMvc
                .perform(post("/playtime")
                        .content(json)
                        .contentType(MediaType.APPLICATION_JSON_VALUE))
                .andExpect(status().isInternalServerError());

        then(service).should().putRecord(json, "playtime");
    }

    private String getJson() throws IOException {
        return IOUtils.toString(getClass().getClassLoader().getResourceAsStream("test.json"));
    }
}
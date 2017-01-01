package com.wartal.aws.kinesisfirehose;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;

/**
 * Created by lwartalski on 01/01/2017.
 */
@RestController
public class Controller {

    private Logger log = LoggerFactory.getLogger(getClass());

    @Autowired
    private KinesisFirehoseService service;

    @RequestMapping(value = "/{deliveryStream}", method = RequestMethod.POST)
    public void putRecord(@RequestBody String json, @PathVariable String deliveryStream, HttpServletRequest request) {
        log.info("Request: remoteAddr: {} deliveryStream: {} body: {}", request.getRemoteAddr(), deliveryStream, json);
        service.putRecord(json, deliveryStream);
    }
}

package com.bit.module.syslog.service;

import org.springframework.amqp.rabbit.annotation.RabbitHandler;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Component;

import static com.bit.module.mqCore.MqConst.MqBaseConst.MQ_MESSAGE;


/**
 * @Description: 消息接收
 * @Author: mifei
 * @Date: 2019-02-15
 **/
@Component
@RabbitListener(queues = MQ_MESSAGE)
public class MessageReceiver {

    @Autowired
    private MongoTemplate mongoTemplate;

    @RabbitHandler
    public void process(String messageStr) {

        //根据日志类型判断应该插入mongodb中的哪个日志表


        //接收到的消息，存入mongodb中
        System.out.println("Topic Receiver1  : " + messageStr);
        mongoTemplate.save(messageStr,"operate_logs");
    }

}

package com.bit.module.syslog.service;

import com.alibaba.fastjson.JSON;
import com.bit.base.exception.BusinessException;
import com.bit.module.mqCore.MqBean.MqDelay;
import com.bit.module.mqCore.MqBean.MqMessage;
import com.bit.module.mqCore.MqConst.MqBaseConst;
import com.bit.common.consts.ApplicationTypeEnum;
import com.bit.module.mqCore.MqEnum.TargetTypeEnum;
import com.bit.module.syslog.feign.VolServiceFeign;
import com.rabbitmq.client.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitHandler;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

import static com.bit.module.mqCore.MqConst.MqBaseConst.MQ_DELAY;

/**
 * @description:
 * @author: chenduo
 * @create: 2019-04-17 11:05
 */
@Component
@RabbitListener(queues = MQ_DELAY)
@Slf4j
public class DelayMessageReceiverNew {


    @Autowired
    private RabbitTemplate rabbitTemplate;
    @Autowired
    private VolServiceFeign volServiceFeign;

    @RabbitHandler
    public void process(Channel channel, String messageDelayStr, Message message) {

        try {
            //告诉服务器已经收到消息 否则消息服务器以为这条消息没处理掉 后续还会在发
            channel.basicAck(message.getMessageProperties().getDeliveryTag(),false);
        }catch (Exception ex){
            throw new BusinessException("消息确认失败");
        }

        log.info("Topic Receiver  : {}",messageDelayStr);
        //发送延迟消息
        MqDelay mqDelay = JSON.parseObject(messageDelayStr,MqDelay.class);

        dealMessageByMQ(mqDelay);

    }

    private void dealMessageByMQ(MqDelay mqDelay){
        Long campaignId = mqDelay.getCampaignId();
        Boolean flag = volServiceFeign.checkCampaignStatusById(campaignId);
        if (flag){
            // 发送mq消息
            MqMessage mqMessage = new MqMessage();
            mqMessage.setAppId(Long.valueOf(ApplicationTypeEnum.APPLICATION_VOL.getApplicationId()));
            mqMessage.setBusinessId(mqDelay.getCampaignId());
            mqMessage.setVersion(0);
            mqMessage.setTemplateId(mqDelay.getTemplateId());
            mqMessage.setTitle("消息提醒");

            List<Long> longList = volServiceFeign.queryEnrollId(campaignId);
            if (longList!=null && longList.size()>0){
                Long[] targetId = new Long[longList.size()];
                longList.toArray(targetId);
                mqMessage.setTargetId(targetId);
            }
            mqMessage.setTargetType(TargetTypeEnum.USER.getCode());
            mqMessage.setTargetUserType(String.valueOf(TargetTypeEnum.EXTERNALUSER.getCode()));
            mqMessage.setParams(mqDelay.getParams());
            String str =  JSON.toJSONString(mqMessage);
            rabbitTemplate.convertAndSend(MqBaseConst.MQ_MESSAGES_EXCHANGE,MqBaseConst.MQ_MESSAGES,str);
        }


    }

}

package com.bit.module.syslog.service;

import com.alibaba.fastjson.JSON;
import com.bit.base.exception.BusinessException;
import com.bit.common.MqConstEnum;
import com.bit.module.mqCore.MqBean.MqDelay;
import com.bit.module.mqCore.MqBean.MqMessage;
import com.bit.module.mqCore.MqBean.MqNoticeMessage;
import com.bit.module.mqCore.MqBean.MsMessage;
import com.bit.module.mqCore.MqConst.MqBaseConst;
import com.bit.module.mqCore.MqEnum.MessageTemplateEnum;
import com.bit.module.mqCore.MqEnum.MsgTypeEnum;
import com.bit.module.mqCore.MqEnum.TargetTypeEnum;
import com.bit.module.syslog.feign.VolServiceFeign;
import com.bit.utils.DateUtil;
import com.rabbitmq.client.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitHandler;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static com.bit.common.MqConstEnum.*;
import static com.bit.module.mqCore.MqConst.MqBaseConst.MQ_DELAY;
import static com.bit.module.mqCore.MqConst.MqBaseConst.MS_JPUSH_EXCHANGE;
import static com.bit.module.mqCore.MqConst.MqBaseConst.MS_MESSAGETOJPUSH;

/**
 * @description:
 * @author: chenduo
 * @create: 2019-04-17 11:05
 */
@Component
//@RabbitListener(queues = MQ_DELAY)
public class DelayMessageReceiver {

    private static final Logger logger = LoggerFactory.getLogger(DelayMessageReceiver.class);

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

        logger.info("Topic Receiver  : {}",messageDelayStr);
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
            mqMessage.setAppId(Long.valueOf(MqConstEnum.VOLORG.getCode()));
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
            mqMessage.setTargetUserType(TargetTypeEnum.EXTERNALUSER.getCode()+"");
            mqMessage.setParams(mqDelay.getParams());
            String str =  JSON.toJSONString(mqMessage);
            rabbitTemplate.convertAndSend(MqBaseConst.MQ_MESSAGES_EXCHANGE,MqBaseConst.MQ_MESSAGES,str);
        }










//        MsMessage msMessage = new MsMessage();
//
//        // 对应的业务表id
//        msMessage.setBusinessId(mqDelay.getCampaignId());
//        //消息类型
//        msMessage.setMsgType(MsgTypeEnum.MSG_TYPE_MESSAGE.getCode());
//        // 消息类型名称
//        msMessage.setMsgTypeName(mqDelay.getMsgTypeName());
//        // 消息显示标题(通知公告列表显示title 其余显示content)
//        msMessage.setContent(messageTemplate);
//        //接入端 3是志愿者app
//        msMessage.setTid("3");
//        //应用ID
//        msMessage.setAppId(mqDelay.getAppId().longValue());
//        //指定人员推送使用别名推送
//        msMessage.setTagType(PUSHBYALIASE.getCode());
//
//        List<Long> longList = volServiceFeign.queryEnrollId(mqDelay.getCampaignId());
//        if (longList!=null && longList.size()>0){
//            Long[] targetIds =new Long[longList.size()];
//            longList.toArray(targetIds);
//            // 用户id
//            msMessage.setUserId(targetIds);
//            //推送至极光
//            String jPushStr = JSON.toJSONString(msMessage);
//            rabbitTemplate.convertAndSend(MS_JPUSH_EXCHANGE,MS_MESSAGETOJPUSH,jPushStr);
//        }

    }

}

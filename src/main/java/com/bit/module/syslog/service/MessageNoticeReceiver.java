package com.bit.module.syslog.service;

import com.alibaba.fastjson.JSON;
import com.bit.base.exception.BusinessException;
import com.bit.module.mqCore.MqBean.MqNoticeMessage;
import com.bit.module.mqCore.MqBean.MsMessage;
import com.bit.module.mqCore.MqBean.UserMessage;
import com.bit.module.syslog.feign.SysServiceFeign;
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

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static com.bit.common.MqConstEnum.*;
import static com.bit.module.mqCore.MqConst.MqBaseConst.*;

/**
 * 通知公告消息处理
 * @author liyang
 * @date 2019-04-09
*/
@Component
//@RabbitListener(queues = MQ_MESSAGETONOCTICE)
public class MessageNoticeReceiver {

    private static final Logger logger = LoggerFactory.getLogger(MessagesReceiver.class);

    /**
     * mongo工具类
     */
    @Autowired
    private MongoTemplate mongoTemplate;

    /**
     * sys微服务
     */
    @Autowired
    private SysServiceFeign sysServiceFeign;

    /**
     * mq工具模板
     */
    @Autowired
    private RabbitTemplate rabbitTemplate;

    @RabbitHandler
    public void process(Channel channel, String messageNoticeStr, Message message) {

        try {
            //告诉服务器已经收到消息 否则消息服务器以为这条消息没处理掉 后续还会在发
            channel.basicAck(message.getMessageProperties().getDeliveryTag(),false);
        }catch (Exception ex){
            throw new BusinessException("消息确认失败");
        }

        logger.info("Topic Receiver  : {}",messageNoticeStr);
        MqNoticeMessage mqNoticeMessage = JSON.parseObject(messageNoticeStr,MqNoticeMessage.class);

        //根据接入端区分是否存入mongo和发送方式
        List<String> tidList = Arrays.asList(mqNoticeMessage.getTid());
        if(tidList.contains(String.valueOf(TERMINALTOWEB.getCode()))){

            //推送至web端处理
            dealMessageByWeb(mqNoticeMessage);

        }

        //如果还存在其余接入端发送
        if(tidList.contains(String.valueOf(TERMINALTOPBAPP.getCode())) ||
                tidList.contains(String.valueOf(TERMINALTOVOLAPP.getCode())) ||
                tidList.contains(String.valueOf(TERMINALTOPIAPP.getCode())) ||
                tidList.contains(String.valueOf(TERMINALTOOAAPP.getCode()))){

            //推送至APP端处理
            dealMessageByJpush(mqNoticeMessage);
        }

    }

    /**
     * 推送至极光
     * @author liyang
     * @date 2019-04-10
     * @param mqNoticeMessage : 通知公告模板
    */
    private void dealMessageByJpush(MqNoticeMessage mqNoticeMessage) {

        List<Long> userIdList = new ArrayList<>();

        //获取TID集合
        List<String> tids = new ArrayList<>();
        for(String tid : mqNoticeMessage.getTid()){
            tids.add(tid);
        }

        if(mqNoticeMessage.getTargetType().equals(PUSHTOALLUSER.getCode())){

            //获取应用ID 查询人员
            //根据TID获取组织 1、党建  2、政务 8、志愿者
            if(mqNoticeMessage.getAppId().equals(PBORG.getCode())){

                //党建获取组织内所有人员
                userIdList = sysServiceFeign.getAllUserIdsForPbOrg();

                //存入mongo
                insertMongo(mqNoticeMessage,userIdList,String.valueOf(TERMINALTOPBAPP.getCode()));

                //推送至msqueue
                sendToMsQueue(mqNoticeMessage,userIdList,String.valueOf(TERMINALTOPBAPP.getCode()));

            }else if(mqNoticeMessage.getAppId().equals(OADEP.getCode())){

                //政务
                userIdList = sysServiceFeign.getAllUserIdsForOaOrg();

                //再次判断是政务APP 还是巡检APP
                if(tids.contains(String.valueOf(TERMINALTOOAAPP.getCode()))){

                    //存入mongo
                    insertMongo(mqNoticeMessage,userIdList,String.valueOf(TERMINALTOOAAPP.getCode()));

                    //政务 推送至msqueue
                    sendToMsQueue(mqNoticeMessage,userIdList,String.valueOf(TERMINALTOOAAPP.getCode()));
                }
                if(tids.contains(String.valueOf(TERMINALTOPIAPP.getCode()))){

                    //存入mongo
                    insertMongo(mqNoticeMessage,userIdList,String.valueOf(TERMINALTOPIAPP.getCode()));

                    //巡检 推送至msqueue
                    sendToMsQueue(mqNoticeMessage,userIdList,String.valueOf(TERMINALTOPIAPP.getCode()));
                }


            }else if(mqNoticeMessage.getAppId().equals(VOLORG.getCode())){

                //志愿者
                userIdList = sysServiceFeign.getAllUserIdForVolOrg();

                //存入mongo
                insertMongo(mqNoticeMessage,userIdList,String.valueOf(TERMINALTOVOLAPP.getCode()));

                //推送至msqueue
                sendToMsQueue(mqNoticeMessage,userIdList,String.valueOf(TERMINALTOVOLAPP.getCode()));

            }

        }else {
            for(Long userId : mqNoticeMessage.getTargetId()){
                userIdList.add(userId);
            }


            if(mqNoticeMessage.getAppId().equals(PBORG.getCode())){

                //存入mongo
                insertMongo(mqNoticeMessage,userIdList,String.valueOf(TERMINALTOPBAPP.getCode()));

                //推送至msqueue
                sendToMsQueue(mqNoticeMessage,userIdList,String.valueOf(TERMINALTOPBAPP.getCode()));

            }else if(mqNoticeMessage.getAppId().equals(OADEP.getCode())){

                //再次判断是政务APP 还是巡检APP
                if(tids.contains(String.valueOf(TERMINALTOOAAPP.getCode()))){

                    //存入mongo
                    insertMongo(mqNoticeMessage,userIdList,String.valueOf(TERMINALTOOAAPP.getCode()));

                    //政务 推送至msqueue
                    sendToMsQueue(mqNoticeMessage,userIdList,String.valueOf(TERMINALTOOAAPP.getCode()));
                }
                if(tids.contains(String.valueOf(TERMINALTOPIAPP.getCode()))){

                    //存入mongo
                    insertMongo(mqNoticeMessage,userIdList,String.valueOf(TERMINALTOPIAPP.getCode()));

                    //巡检 推送至msqueue
                    sendToMsQueue(mqNoticeMessage,userIdList,String.valueOf(TERMINALTOPIAPP.getCode()));
                }

            }else if(mqNoticeMessage.getAppId().equals(VOLORG.getCode())){

                //存入mongo
                insertMongo(mqNoticeMessage,userIdList,String.valueOf(TERMINALTOVOLAPP.getCode()));

                //推送至msqueue
                sendToMsQueue(mqNoticeMessage,userIdList,String.valueOf(TERMINALTOVOLAPP.getCode()));

            }
        }
    }

    /**
     * 推送至ms
     * @param mqNoticeMessage
     * @param userIdList
     */
    private void sendToMsQueue(MqNoticeMessage mqNoticeMessage,List<Long> userIdList,String tid) {
        MsMessage msMessage = new MsMessage();

        // 对应的业务表id
        msMessage.setBusinessId(mqNoticeMessage.getBusinessId());

        // 用户id
        Long[] userIds = new Long[userIdList.size()];
        msMessage.setUserId(userIdList.toArray(userIds));

        //消息类型
        msMessage.setMsgType(mqNoticeMessage.getMsgType());

        // 消息类型名称
        msMessage.setMsgTypeName(mqNoticeMessage.getMsgTypeName());

        // 消息显示标题(通知公告列表显示title 其余显示content)
        msMessage.setContent(mqNoticeMessage.getTitle());

        //应用ID
        msMessage.setAppId(mqNoticeMessage.getAppId().longValue());

        //web推送不存在 别名推送和标签推送
        if(tid.equals(String.valueOf(TERMINALTOWEB.getCode()))){
            // 接入端 1 web接入端
            msMessage.setTid(String.valueOf(TERMINALTOWEB.getCode()));

            //推送至websocket
            String webSocketStr = JSON.toJSONString(msMessage);
            rabbitTemplate.convertAndSend(MS_WEB_EXCHANGE,MS_MESSAGETOWEB,webSocketStr);
        }else{
            //推送至APP
            //判断是否是全部人员推送
            if(mqNoticeMessage.getTargetType().equals(PUSHTOALLUSER.getCode())){

                //全部人员推送 使用标签推送
                msMessage.setTagType(PUSHBYTAG.getCode());
            }else {

                //指定人员推送使用别名推送
                msMessage.setTagType(PUSHBYALIASE.getCode());
            }

            //存入接入端
            msMessage.setTid(tid);

            //获取title
            msMessage.setTitle(mqNoticeMessage.getTitle());

            //推送至极光
            String jPushStr = JSON.toJSONString(msMessage);
            rabbitTemplate.convertAndSend(MS_JPUSH_EXCHANGE,MS_MESSAGETOJPUSH,jPushStr);

        }
    }

    /**
     * 根据APPID获取所有人存入mongoDB
     * @author liyang
     * @date 2019-04-10
     * @param mqNoticeMessage : 消息明细
    */
    private void dealMessageByWeb(MqNoticeMessage mqNoticeMessage) {

        //  1为使用tag, 给所有用户推送 2、指定人
        List<Long> userIdList = new ArrayList<>();
        if(mqNoticeMessage.getTargetType().equals(PUSHTOALLUSER.getCode())){

            //获取应用ID 查询人员
            //根据appId获取组织 1、党建  2、政务 8、志愿者
            if(mqNoticeMessage.getAppId().equals(PBORG.getCode())){

                //党建获取组织内所有人员
                userIdList = sysServiceFeign.getAllUserIdsForPbOrg();

                //存入mongo
                insertMongo(mqNoticeMessage,userIdList,String.valueOf(TERMINALTOWEB.getCode()));

                //推送至webSocket
                sendToMsQueue(mqNoticeMessage,userIdList,String.valueOf(TERMINALTOWEB.getCode()));

            }else if(mqNoticeMessage.getAppId().equals(OADEP.getCode())){

                //政务
                userIdList = sysServiceFeign.getAllUserIdsForOaOrg();

                //存入mongo
                insertMongo(mqNoticeMessage,userIdList,String.valueOf(TERMINALTOWEB.getCode()));

                //推送至webSocket
                sendToMsQueue(mqNoticeMessage,userIdList,String.valueOf(TERMINALTOWEB.getCode()));

            }else if(mqNoticeMessage.getAppId().equals(VOLORG.getCode())){

                //志愿者
                userIdList = sysServiceFeign.getAllUserIdForVolOrg();

                //存入mongo
                insertMongo(mqNoticeMessage,userIdList,String.valueOf(TERMINALTOWEB.getCode()));

                //推送至webSocket
                sendToMsQueue(mqNoticeMessage,userIdList,String.valueOf(TERMINALTOWEB.getCode()));
            }

        }else {
            for(Long userId : mqNoticeMessage.getTargetId()){
                userIdList.add(userId);
            }

            //存入mongo
            insertMongo(mqNoticeMessage,userIdList,String.valueOf(TERMINALTOWEB.getCode()));

            //推送至webSocket
            sendToMsQueue(mqNoticeMessage,userIdList,String.valueOf(TERMINALTOWEB.getCode()));
        }
    }

    /**
     * 根据userId存入mongoDB
     * @param mqNoticeMessage 通知明细
     * @param userIdList 推送人群
     */
    private void insertMongo(MqNoticeMessage mqNoticeMessage,List<Long> userIdList,String tid) {
        List<UserMessage> userMessageList = new ArrayList<>();

        //根据人物ID循环
        for (Long userId : userIdList){

            UserMessage um = new UserMessage();

            //appId
            um.setAppid(mqNoticeMessage.getAppId());

            //业务表ID
            um.setBusinessId(mqNoticeMessage.getBusinessId());

            //消息显示标题(通知公告列表显示title 其余显示content)
            um.setContent(mqNoticeMessage.getTitle());

            //是否已读 0 未读  1 已读
            um.setStatus(UNREAD.getCode());

            //userId
            um.setUserId(userId);

            //接入端
            um.setTid(tid);

            //创建人
            um.setCreater(mqNoticeMessage.getCreater());

            //消息类型 1消息 2待办 3通知 4公告 5已办
            um.setMsgType(mqNoticeMessage.getMsgType());

            //消息类型名称
            um.setMsgTypeName(mqNoticeMessage.getMsgTypeName());

            //消息创建时间
            Date currentTime = new Date();
            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String dateString = formatter.format(currentTime);
            um.setRecTime(dateString);

            userMessageList.add(um);

        }

        //存入mongo
        mongoTemplate.insert(userMessageList,MONGO_MESSAGE_COLLECTION);

    }

}

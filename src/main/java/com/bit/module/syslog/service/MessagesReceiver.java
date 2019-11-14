package com.bit.module.syslog.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.bit.base.exception.BusinessException;
import com.bit.base.vo.BaseVo;
import com.bit.module.mqCore.MqBean.MqMessage;
import com.bit.module.mqCore.MqBean.MsMessage;
import com.bit.module.mqCore.MqBean.UserMessage;
import com.bit.module.syslog.bean.MessageTemplate;
import com.bit.module.syslog.feign.SysServiceFeign;
import com.bit.module.syslog.feign.VolServiceFeign;
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

import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.*;

import static com.bit.common.MqConstEnum.*;
import static com.bit.module.mqCore.MqConst.MqBaseConst.*;

/**
 * @Description: 待办等消息处理
 * @Author: liyang
 * @Date: 2019-04-03
 **/
@Component
//@RabbitListener(queues = MQ_MESSAGES)
public class MessagesReceiver {

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
     * 志愿者微服务
     */
    @Autowired
    private VolServiceFeign volServiceFeign;

    /**
     * mq工具模板
     */
    @Autowired
    private RabbitTemplate rabbitTemplate;

    /**
     * 消息处理
     * @param messageStr
     */
    @RabbitHandler
    public void process(Channel channel, String messageStr, Message message) {
        try {
            //告诉服务器已经收到消息 否则消息服务器以为这条消息没处理掉 后续还会在发
            channel.basicAck(message.getMessageProperties().getDeliveryTag(),false);
        }catch (Exception ex){
            throw new  BusinessException("消息确认失败");
        }

        logger.info("Topic Receiver  : {}",messageStr);
        MqMessage mqMessage = JSON.parseObject(messageStr,MqMessage.class);

        //根据模板类型获取模板详情
        BaseVo baseVo = sysServiceFeign.getMessageTempByMessageTempId(mqMessage.getTemplateId());
        String strMessageTemp = JSON.toJSONString(baseVo.getData());
        MessageTemplate messageTemplate = JSON.parseObject(strMessageTemp,MessageTemplate.class);

        //判断消息发布的类型是组织还是具体人
        if(mqMessage.getTargetType().equals(PUSHTOORGANIZATION.getCode())){

            //如果为组织，先根据模板ID查询对应APPID
            Integer appId = messageTemplate.getAppId();
            mqMessage.setAppId(appId.longValue());

            //组织插入
            List<Long> orgList = new ArrayList<>();
            for(Long orgId : mqMessage.getTargetId()){
                orgList.add(orgId);
            }
            messageTemplate.setOrgIds(orgList);

            //根据appId获取组织 1、党建  2、政务 8、志愿者
            if(appId.equals(PBORG.getCode())){

                //如果为党建组织，党建消息处理
                pbMessageHaldling(mqMessage,messageTemplate);
            }else if(appId.equals(OADEP.getCode())){

                //如果为政务
                oaMessageHaldling(mqMessage,messageTemplate);
            }else if(appId.equals(VOLORG.getCode())){

                //如果为志愿者组织
                volMessageHalding(mqMessage,messageTemplate);
            }

        }else{
            //如果为具体人员
            //拼接模板
            Map<String,String> templateMap = getMessageTemplate(messageTemplate,mqMessage);

            //根据模板获取接入端ID
            List<String> tidList = sysServiceFeign.getTidByMessageTemplate(messageTemplate);

            List<Long> userIdList = new ArrayList<>();
            for(Long userId : mqMessage.getTargetId()){
                userIdList.add(userId);

            }

            logger.info("模板拼接完毕.....模板是....."+templateMap+" 推送的人是....." + userIdList);

            //存入mongo
            insertToMongo(messageTemplate,mqMessage,userIdList,templateMap,tidList);

            //发送MS
            sendToMs(messageTemplate,mqMessage,userIdList,templateMap,tidList);
        }
    }

    /**
     * 志愿者组织消息处理
     * @author liyang
     * @date 2019-04-09
     * @param mqMessage : 需要处理的消息
     * @param messageTemplate : 消息模板详情
    */
    private void volMessageHalding(MqMessage mqMessage, MessageTemplate messageTemplate) {
        BaseVo baseVo = new BaseVo();

        List<Long> userIdList = new ArrayList<>();

        //根据消息类型，获取具体人员 1、内部用户  2、外部用户
        if(messageTemplate.getUserType().equals(EXTERNALUSER.getCode())){
            userIdList = volServiceFeign.getAllVolUserIds();
        }else if(messageTemplate.getUserType().equals(INTERNALUSER.getCode())){
            baseVo = sysServiceFeign.getAdminUserByStationOrgIds(messageTemplate);
            String userIds = JSON.toJSONString(baseVo.getData());
            userIdList = JSONArray.parseArray(userIds,java.lang.Long.class);
        }

        //拼接模板
        Map<String,String> templateMap = getMessageTemplate(messageTemplate,mqMessage);

        //根据模板获取接入端ID
        List<String> tidList = sysServiceFeign.getTidByMessageTemplate(messageTemplate);

        //存入mongo
        insertToMongo(messageTemplate,mqMessage,userIdList,templateMap,tidList);

        //发送MS
        sendToMs(messageTemplate,mqMessage,userIdList,templateMap,tidList);

    }

    /**
     * 政务组织消息处理
     * @author liyang
     * @date 2019-04-09
     * @param mqMessage : 需要处理的消息
     * @param messageTemplate : 消息模板详情
    */
    private void oaMessageHaldling(MqMessage mqMessage, MessageTemplate messageTemplate) {

        List<Long> userIdList = new ArrayList<>();

        //判断是组织还是所有人
        if(messageTemplate.getUserType().equals(PUSHTOORGANIZATION.getCode())){

            //所有人
            userIdList = sysServiceFeign.getAllUserIdsForOaOrg();
        }else{

            //获取组织内所有人员
            BaseVo baseVo = sysServiceFeign.getAllUserIdsByOaOrgIds(mqMessage.getTargetId());
            String userIds = JSON.toJSONString(baseVo.getData());
            userIdList = JSONArray.parseArray(userIds,java.lang.Long.class);
        }

        //拼接模板
        Map<String,String> templateMap = getMessageTemplate(messageTemplate,mqMessage);

        //根据模板获取接入端ID
        List<String> tidList = sysServiceFeign.getTidByMessageTemplate(messageTemplate);

        //存入mongo
        insertToMongo(messageTemplate,mqMessage,userIdList,templateMap,tidList);

        //发送MS
        sendToMs(messageTemplate,mqMessage,userIdList,templateMap,tidList);
    }

    /**
     * 党建组织消息处理
     * @author liyang
     * @date 2019-04-04
     * @param mqMessage : 需要处理的消息
     * @param messageTemplate : 消息模板详情
    */
    private void pbMessageHaldling(MqMessage mqMessage,MessageTemplate messageTemplate) {
        BaseVo baseVo = new BaseVo();

        List<Long> userIdList = new ArrayList<>();

        //如果是组织，判断消息类型 0、全部 1、内部用户  2、外部用户
        if(messageTemplate.getUserType().equals(PUSHTOALLUSER.getCode())){

            //获取组织内所有人员
            baseVo = sysServiceFeign.getAllUserIdsByPbOrgIds(mqMessage.getTargetId());
            String userIds = JSON.toJSONString(baseVo.getData());
            userIdList = JSONArray.parseArray(userIds,java.lang.Long.class);
        }else{

            //如果是内部或者外部用户
            baseVo = sysServiceFeign.getUserIdsByOrgIds(messageTemplate);
            String userIds = JSON.toJSONString(baseVo.getData());
            userIdList = JSONArray.parseArray(userIds,java.lang.Long.class);
        }

        //拼接模板
        Map<String,String> templateMap = getMessageTemplate(messageTemplate,mqMessage);

        //根据模板获取接入端ID
        List<String> tidList = sysServiceFeign.getTidByMessageTemplate(messageTemplate);

        //存入mongo
        insertToMongo(messageTemplate,mqMessage,userIdList,templateMap,tidList);

        //发送MS
        sendToMs(messageTemplate,mqMessage,userIdList,templateMap,tidList);
    }

    /**
     * 组装推送至相应的消息队列
     * @author liyang
     * @date 2019-04-08
     * @param messageTemplate ：消息模板详情
     * @param mqMessage ：mq队列内容
     * @param userIdList ：userId用户集合
     * @param templateMap ：消息内容和标题
     * @param tidList ：接入端集合
    */
    private void sendToMs(MessageTemplate messageTemplate, MqMessage mqMessage, List<Long> userIdList, Map<String,String> templateMap, List<String> tidList) {
        MsMessage msMessage = new MsMessage();
        //对应的业务表id
        if(mqMessage.getBusinessId() != null && !mqMessage.getBusinessId().equals("")){
            msMessage.setBusinessId(mqMessage.getBusinessId());
        }

        //用户集合
        Long[] userIds = new Long[userIdList.size()];
        msMessage.setUserId(userIdList.toArray(userIds));

        //消息使用别名发送，不存在全体推送的情况  1为使用tag, 给所有用户推送 2 别名推送
        msMessage.setTagType(PUSHBYALIASE.getCode());

        //消息类型
        msMessage.setMsgType(messageTemplate.getMsgType());

        //消息内容
        msMessage.setContent(templateMap.get(MESSAGECONTENT.getInfo()));

        //类目code
        msMessage.setCategoryCode(messageTemplate.getCategory());

        //应用ID
        msMessage.setAppId(mqMessage.getAppId());

        if(mqMessage.getPushTime()!= null && !mqMessage.getPushTime().equals("")){
            msMessage.setTime(mqMessage.getPushTime());
        }

        //先检测是否有推送至websocket
        if(tidList.contains(String.valueOf(TERMINALTOWEB.getCode()))){

            msMessage.setTid(String.valueOf(TERMINALTOWEB.getCode()));
            String webSocketStr = JSON.toJSONString(msMessage);

            //推送至websocket
            rabbitTemplate.convertAndSend(MS_WEB_EXCHANGE,MS_MESSAGETOWEB,webSocketStr);

        }

        //再检测是否有推送至其余APP的
        if(tidList.contains(String.valueOf(TERMINALTOPBAPP.getCode()))
                || tidList.contains(String.valueOf(TERMINALTOVOLAPP.getCode()))
                || tidList.contains(String.valueOf(TERMINALTOPIAPP.getCode()))
                || tidList.contains(String.valueOf(TERMINALTOOAAPP.getCode()))
                || tidList.contains(String.valueOf(TERMINALTOSVAPP.getCode()))
                || tidList.contains(String.valueOf(TERMINALTOEPAPP.getCode()))
                || tidList.contains(String.valueOf(TERMINALTOUCAPP.getCode()))){

            //推送至APP
            //获取tid 推送tid 不等于1 的所有APP
            for(String tid : tidList){
                if(!tid.equals(String.valueOf(TERMINALTOWEB.getCode()))){
                    msMessage.setTid(tid);

                    //APP地址
                    msMessage.setAppUrl(messageTemplate.getAppUrl());

                    //是否需要跳转
                    msMessage.setJumpFlag(messageTemplate.getJumpFlag());

                    //获取title
                    msMessage.setTitle(templateMap.get(MESSAGETITLE.getInfo()));
                    String jPushStr = JSON.toJSONString(msMessage);

                    //推送至极光
                    rabbitTemplate.convertAndSend(MS_JPUSH_EXCHANGE,MS_MESSAGETOJPUSH,jPushStr);
                }
            }
        }
    }

    /**
     * 将消息根据人员和接入端插入mongo
     * @author liyang
     * @date 2019-04-08
     * @param messageTemplate : 消息模板详情
     * @param mqMessage :  mq消息详情
     * @param userIdList : 发送人详情
     * @param templateMap : 发送内容和标题
     * @param tidList : 接入端集合
    */
    private void insertToMongo(MessageTemplate messageTemplate, MqMessage mqMessage,List<Long> userIdList,Map<String,String> templateMap,List<String> tidList) {

        List<UserMessage> userMessagesList = new ArrayList<>();

        //拼接mongo 先循环userId
        for (int i = 0; i < userIdList.size(); i++) {

            //再循环tidList
            for(String tid : tidList){

                UserMessage um = new UserMessage();

                //appId
                um.setAppid(messageTemplate.getAppId());

                //业务表ID
                if(mqMessage.getBusinessId() != null && !mqMessage.getBusinessId().equals("")){
                    um.setBusinessId(mqMessage.getBusinessId());
                }

                //消息内容
                um.setContent(templateMap.get(MESSAGECONTENT.getInfo()));

                //是否已读 0 未读  1 已读
                um.setStatus(UNREAD.getCode());

                //userId
                um.setUserId(userIdList.get(i));

                //接入端
                um.setTid(tid);

                //创建人
                um.setCreater(mqMessage.getCreater());

                //消息类型 1消息 2待办 3通知 4公告 5已办
                um.setMsgType(messageTemplate.getMsgType());

                //消息类型名称
                um.setMsgTypeName(templateMap.get(MESSAGETYPENAME.getInfo()));

                //消息创建时间
                Date currentTime = new Date();
                SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                String dateString = formatter.format(currentTime);
                um.setRecTime(dateString);

                //类目ID
                um.setCategoryCode(messageTemplate.getCategory());

                //类目名称
                um.setCategoryName(templateMap.get(MESSAGECATEGORYNAME.getInfo()));

                //锁
                um.setVersion(mqMessage.getVersion());

                //一级菜单
                um.setLevelOneMenu(messageTemplate.getLevelOneMenu());

                userMessagesList.add(um);
            }
        }
        //存入mongo
        mongoTemplate.insert(userMessagesList,MONGO_MESSAGE_COLLECTION);
        logger.info("存入mongo数据是：" + userMessagesList);
    }

    /**
     * 拼接模板实际内容
     * @author liyang
     * @date 2019-04-08
     * @param messageTemplate 模板
     * @return 模板标题和模板内容
    */
    private Map<String,String> getMessageTemplate(MessageTemplate messageTemplate,MqMessage msMessage) {
        String content = null;

        //根据消息参数替换相应的消息内容
        switch (msMessage.getParams().length){
            case 1:
                content = MessageFormat.format(messageTemplate.getContext() ,msMessage.getParams()[0]);
                break;
            case 2:
                content = MessageFormat.format(messageTemplate.getContext() ,msMessage.getParams()[0],msMessage.getParams()[1]);
                break;
            case 3:
                content = MessageFormat.format(messageTemplate.getContext() ,msMessage.getParams()[0],msMessage.getParams()[1],msMessage.getParams()[2]);
                break;
            case 4:
                content = MessageFormat.format(messageTemplate.getContext() ,msMessage.getParams()[0],msMessage.getParams()[1],msMessage.getParams()[2],msMessage.getParams()[3]);
                break;
            case 5:
                content = MessageFormat.format(messageTemplate.getContext() ,msMessage.getParams()[0],msMessage.getParams()[1],msMessage.getParams()[2],msMessage.getParams()[3],msMessage.getParams()[4]);
                break;
            case 6:
                content = MessageFormat.format(messageTemplate.getContext() ,msMessage.getParams()[0],msMessage.getParams()[1],msMessage.getParams()[2],msMessage.getParams()[3],msMessage.getParams()[4],msMessage.getParams()[5]);
                break;
            default:
                content = messageTemplate.getContext();
                break;
        }

        Map<String,String> messageMap = new HashMap<>();
        messageMap.put(MESSAGECONTENT.getInfo(),content);

        //获取消息标题
        Map<String,String> titleMap = sysServiceFeign.getMessageTitle(messageTemplate);
        String messageTitle = titleMap.get(MESSAGETYPENAME.getInfo()) + "(" + titleMap.get(MESSAGECATEGORYNAME.getInfo()) + ")";
        messageMap.put(MESSAGETITLE.getInfo(),messageTitle);

        //获取消息类目名称
        messageMap.put(MESSAGETYPENAME.getInfo(),titleMap.get(MESSAGETYPENAME.getInfo()));

        //获取消息类型名称
        messageMap.put(MESSAGECATEGORYNAME.getInfo(),titleMap.get(MESSAGECATEGORYNAME.getInfo()));

        return messageMap;
    }

}

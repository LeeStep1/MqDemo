package com.bit.module.syslog.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.bit.base.exception.BusinessException;
import com.bit.base.vo.BaseVo;
import com.bit.module.mqCore.MqBean.MqMessage;
import com.bit.module.mqCore.MqBean.MsMessage;
import com.bit.module.mqCore.MqBean.UserMessage;
import com.bit.module.mqCore.MqEnum.AppPushTypeEnum;
import com.bit.common.consts.ApplicationTypeEnum;
import com.bit.module.mqCore.MqEnum.TargetTypeEnum;
import com.bit.common.consts.TerminalTypeEnum;
import com.bit.module.syslog.bean.MessageTemplate;
import com.bit.module.syslog.bean.MessageTemplateRelTid;
import com.bit.module.syslog.feign.SysServiceFeign;
import com.bit.module.syslog.feign.VolServiceFeign;
import com.rabbitmq.client.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitHandler;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.*;

import static com.bit.common.MqConstEnum.*;
import static com.bit.module.mqCore.MqConst.MqBaseConst.*;
import static com.bit.module.mqCore.MqConst.MqBaseConst.MS_MESSAGETOJPUSH;

/**
 * @Description:  mq对应的
 * @Author: liyujun
 * @Date: 2019-08-28
 **/
@Component
//@RabbitListener(queues = MQ_MESSAGES+"_lyj")
@RabbitListener(queues = MQ_MESSAGES)
@Slf4j
public class MessagesReceiverNew {



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
     * 监听消息，待办
     * 消息处理
     * @param messageStr
     */
    @RabbitHandler
    public void process(Channel channel, String messageStr, Message message) {
        try {
            //告诉服务器已经收到消息 否则消息服务器以为这条消息没处理掉 后续还会在发
            channel.basicAck(message.getMessageProperties().getDeliveryTag(),false);
            log.info("Topic_topic.messages Receiver  : {}",messageStr);
        }catch (Exception ex){
            log.error("消费失败 {}",ex.getStackTrace());
        }

        MqMessage mqMessage = JSON.parseObject(messageStr,MqMessage.class);

        //根据模板类型获取模板详情
        BaseVo baseVo = sysServiceFeign.getMessageTempByMessageTempId(mqMessage.getTemplateId());
        String strMessageTemp = JSON.toJSONString(baseVo.getData());
        MessageTemplate messageTemplate = JSON.parseObject(strMessageTemp,MessageTemplate.class);
        //进行处理具体的业务：组织转换人员，推送方式区分
        handleMessges(mqMessage,messageTemplate);

    }

    /**
     * @description:  数据模板进行拼装
     * @author liyujun
     * @date 2019-08-29
     * @param mqMessage :
     * @param messageTemplate :
     * @return : void
     */
    private void handleMessges(MqMessage mqMessage, MessageTemplate messageTemplate) {
        List<Long> userIdList = new ArrayList<>();
        //拼接模板
        Map<String,String> templateMap = getMessageTemplate(messageTemplate,mqMessage);

        List<MessageTemplateRelTid> list=sysServiceFeign.getTemplateTidConfig(messageTemplate);

        List <String>tids=new ArrayList<String>();

        List <String>storeTids=new ArrayList<String>();

        list.stream().forEach(ii->{
            if(ii.getStore().equals(0)){
                storeTids.add(ii.getTid());
            }
            tids.add(ii.getTid());
        });
        //根据模板获取接入端ID
        if(mqMessage.getAppJpushType()== AppPushTypeEnum.PUSHBYALIASE.getPushCode() /*&& mqMessage.getTargetType()!=TargetTypeEnum.ALL.getCode()*/){
            //别名推送的判断条件依据是：默认推送方式别名推送并且选择的推送目标不能为应用全部
            //以下逻辑为：别名推送组装人员
            //如果推送的目标选择的是组织的话，要进行组织用户  PUSHTOORGANIZATION(2,"组织"),
            //if(mqMessage.getTargetType().equals(PUSHTOORGANIZATION.getCode()))
            if(mqMessage.getTargetType().equals(TargetTypeEnum.ORG.getCode())){

                Integer appId = messageTemplate.getAppId();

                mqMessage.setAppId(appId.longValue());
                //组织插入
                messageTemplate.setOrgIds(Arrays.asList(mqMessage.getTargetId()));
                //根据不同应用取用户，远程调用取用户
                //根据appId获取组织 1、党建  2、政务 8、志愿者
                //if(appId.equals(PBORG.getCode()))
                //    PBORG(1,"党建组织"),
                if(appId.equals(ApplicationTypeEnum.APPLICATION_PB.getApplicationId())){
                    //党建应用人员逻辑
                    userIdList= pbMessageHaldling(mqMessage,messageTemplate);
                    //appId.equals(OADEP.getCode())
                    //    OADEP(2,"政务组织"),
                }else if(appId.equals(ApplicationTypeEnum.APPLICATION_OA.getApplicationId())){
                    //政务应用人员逻辑
                    userIdList=  oaMessageHaldling(mqMessage,messageTemplate);

                    // if(appId.equals(VOLORG.getCode()))    VOLORG(8,"志愿者组织");
                }else if(appId.equals(ApplicationTypeEnum.APPLICATION_VOL.getApplicationId())){
                    //志愿者应用
                    userIdList=  volMessageHalding(mqMessage,messageTemplate);
                }
                //todo  社区应用模块逻辑用户
            }else{
                //使用的是用户
                userIdList= Arrays.asList(mqMessage.getTargetId());

            }
            log.info("别名推送的初始化的目标用户{}",userIdList);
            //目前只支持别名推送判断是否存，如果标签推送，则为不存
            //todo 存储的只能是别名推送，并且需要改造模板与接入端数据表，增加一列
            if (storeTids.size()>0){
                insertToMongo(messageTemplate,mqMessage,userIdList,templateMap,storeTids);
            }

        }else{
            //标签推送,需要重置下推送方式，因为之前客户端调用时，无推送模式的字段，并且都是拿推送目标都是全部用户来判断的是否使用标签（tid）
            mqMessage.setAppJpushType(AppPushTypeEnum.PUSHBYTAG.getPushCode());
        }
        log.info("模板拼接完毕.....模板是....."+templateMap+" 推送的人是....." + userIdList);
        try {
            //发送MS
            sendToMs(messageTemplate,mqMessage,userIdList,templateMap,tids);
        }catch (Exception e){
            log.error("mq发送失败{}",e.getMessage());
        }

    }
    /**
     * 志愿者组织消息处理
     * @author liyang
     * @date 2019-04-09
     * @param mqMessage : 需要处理的消息
     * @param messageTemplate : 消息模板详情
     */
    private List<Long> volMessageHalding(MqMessage mqMessage, MessageTemplate messageTemplate) {

        List<Long> userIdList = new ArrayList<>();
        // EXTERNALUSER(2,"外部用户"), INTERNALUSER(1,"内部用户"),
        //根据消息类型，获取具体人员 1、内部用户  2、外部用户
        if(messageTemplate.getUserType().equals(TargetTypeEnum.EXTERNALUSER.getCode())){
            userIdList = volServiceFeign.getAllVolUserIds();
        } else if(messageTemplate.getUserType().equals(TargetTypeEnum.INTERNALUSER.getCode())){
            BaseVo baseVo  = sysServiceFeign.getAdminUserByStationOrgIds(messageTemplate);
            String userIds = JSON.toJSONString(baseVo.getData());
            userIdList = JSONArray.parseArray(userIds,java.lang.Long.class);
        }
        return  userIdList;

    }

    /**
     * 政务组织消息处理
     * @author liyang
     * @date 2019-04-09
     * @param mqMessage : 需要处理的消息
     * @param messageTemplate : 消息模板详情
     */
    private List<Long> oaMessageHaldling(MqMessage mqMessage, MessageTemplate messageTemplate) {

        List<Long> userIdList = new ArrayList<>();

        //   PUSHTOORGANIZATION(2,"组织"),
        // if(messageTemplate.getUserType().equals(PUSHTOORGANIZATION.getCode()))
        if(messageTemplate.getUserType().equals(TargetTypeEnum.ORG.getCode())){
            //所有人
            userIdList = sysServiceFeign.getAllUserIdsForOaOrg();
        }else if(messageTemplate.getUserType().equals(TargetTypeEnum.INTERNALUSER)){
            //获取组织内所有人员
            BaseVo baseVo = sysServiceFeign.getAllUserIdsByOaOrgIds(mqMessage.getTargetId());
            String userIds = JSON.toJSONString(baseVo.getData());
            userIdList = JSONArray.parseArray(userIds,java.lang.Long.class);
        }else{
            //目前还未有外部用户来调用
        }
        return userIdList;
    }

    /**
     * 党建组织消息处理
     * @author liyang
     * @date 2019-04-04
     * @param mqMessage : 需要处理的消息
     * @param messageTemplate : 消息模板详情
     */
    private  List<Long> pbMessageHaldling(MqMessage mqMessage,MessageTemplate messageTemplate) {



        //如果是组织，判断消息类型 0、全部 1、内部用户  2、外部用户
       /* if(messageTemplate.getUserType().equals(PUSHTOALLUSER.getCode())){

            //获取组织内所有人员
            baseVo = sysServiceFeign.getAllUserIdsByPbOrgIds(mqMessage.getTargetId());
            String userIds = JSON.toJSONString(baseVo.getData());
            userIdList = JSONArray.parseArray(userIds,java.lang.Long.class);
        }else{

            //如果是内部或者外部用户
            baseVo = sysServiceFeign.getUserIdsByOrgIds(messageTemplate);
            String userIds = JSON.toJSONString(baseVo.getData());
            userIdList = JSONArray.parseArray(userIds,java.lang.Long.class);
        }*/
        BaseVo baseVo = null;
        List<Long> userIdList = new ArrayList<>();
       // if(!messageTemplate.getUserType().equals(TargetTypeEnum.ORG.getCode())){
        if(!messageTemplate.getUserType().equals(TargetTypeEnum.ALL.getCode())){
            //如果為内部或外部用戶時，查詢
            baseVo = sysServiceFeign.getUserIdsByOrgIds(messageTemplate);

        }else{
            //如果是所有用户
            baseVo = sysServiceFeign.getAllUserIdsByPbOrgIds(mqMessage.getTargetId());

        }
        if(baseVo!=null){
            String userIds = JSON.toJSONString(baseVo.getData());
            userIdList = JSONArray.parseArray(userIds,java.lang.Long.class);
        }

        return userIdList;
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
        if(mqMessage.getBusinessId()!=null ){
            msMessage.setBusinessId(mqMessage.getBusinessId());
        }
        //消息使用别名发送，不存在全体推送的情况  1为使用tag, 给所有用户推送 2 别名推送
        //增加推送方式的判断
        if(mqMessage.getAppJpushType()==AppPushTypeEnum.PUSHBYTAG.getPushCode()){
            //标签推送
            //PUSHBYTAG(1,"标签推送"),
            //msMessage.setTagType(PUSHBYTAG.getCode());
            msMessage.setTagType(AppPushTypeEnum.PUSHBYTAG.getPushCode());
            msMessage.setTagList(mqMessage.getTagList());

        }else{
            //msMessage.setTagType(PUSHBYALIASE.getCode());
            // PUSHBYALIASE(2,"别名推送"),
            //目前改为0
            msMessage.setTagType(AppPushTypeEnum.PUSHBYALIASE.getPushCode());
            //用户集合
            if (userIdList.size()>0){
                Long[] userIds = new Long[userIdList.size()];
                msMessage.setUserId(userIdList.toArray(userIds));
            }else{
                throw new BusinessException("别名推送人员为空");
            }

        }

        //消息类型
        msMessage.setMsgType(messageTemplate.getMsgType());

        //消息内容
        //msMessage.setContent(templateMap.get(MESSAGECONTENT.getInfo()));
        msMessage.setContent(templateMap.get("content"));

        //类目code
        msMessage.setCategoryCode(messageTemplate.getCategory());

        //应用ID
        msMessage.setAppId(mqMessage.getAppId());

        if(!StringUtils.isEmpty(mqMessage.getPushTime()) ){
            msMessage.setTime(mqMessage.getPushTime());
        }

        //先检测是否有推送至websocket
        if(tidList.contains(String.valueOf(TerminalTypeEnum.TERMINALTOWEB.getTid()))){

            msMessage.setTid(String.valueOf(TerminalTypeEnum.TERMINALTOWEB.getTid()));
            String webSocketStr = JSON.toJSONString(msMessage);

            //推送至websocket
            rabbitTemplate.convertAndSend(MS_WEB_EXCHANGE,MS_MESSAGETOWEB,webSocketStr);

        }

        //再检测是否有推送至其余APP的
        if(TerminalTypeEnum.containApp(tidList)){
            //推送至APP
            //获取tid 推送tid 不等于1 的所有APP
            String TERMINALTOWEB=String.valueOf(TerminalTypeEnum.TERMINALTOWEB.getTid());
            for(String tid : tidList){
                if(!tid.equals(TERMINALTOWEB)){
                    msMessage.setTid(tid);
                    //APP地址
                    msMessage.setAppUrl(messageTemplate.getAppUrl());

                    //是否需要跳转
                    msMessage.setJumpFlag(messageTemplate.getJumpFlag());

                    //获取title
                    msMessage.setTitle(templateMap.get("title"));
                    String jPushStr = JSON.toJSONString(msMessage);
                    log.info("发送消息到推送服务{}",jPushStr);
                    //推送至极光
                   // rabbitTemplate.convertAndSend(MS_JPUSH_EXCHANGE+"_lyj",MS_MESSAGETOJPUSH+"_lyj",jPushStr);
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
                if(!StringUtils.isEmpty(mqMessage.getBusinessId())){
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
        //mongoTemplate.insert(userMessagesList,MONGO_MESSAGE_COLLECTION+"1");
        mongoTemplate.insert(userMessagesList,MONGO_MESSAGE_COLLECTION);
        log.info("存入mongo数据是：" + userMessagesList);
    }

    /**
     * 拼接模板实际内容
     * @author liyang
     * @date 2019-04-08
     * @param messageTemplate 模板
     * @return 模板标题和模板内容
     */
    private Map<String,String> getMessageTemplate(MessageTemplate messageTemplate,MqMessage msMessage) {

        String content = "";
        if(msMessage.getParams().length>0){
            content = MessageFormat.format(messageTemplate.getContext() ,msMessage.getParams());
        }else {
            content = messageTemplate.getContext();
        }
        Map<String,String> messageMap = new HashMap<>(4);

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

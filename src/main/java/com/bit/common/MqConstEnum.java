package com.bit.common;

/**
 * MQ枚举类
 * @author Liy
 * @date 2019-04-03
 */
public enum MqConstEnum {

    /**
     * 消息类型名称
     */
    MESSAGETYPENAME(1,"msgTypeName"),

    /**
     * 消息类目名称
     */
    MESSAGECATEGORYNAME(2,"categoryName"),


    /**
     * 标签推送
     */
    PUSHBYTAG(1,"标签推送"),

    /**
     * 别名推送
     */
    PUSHBYALIASE(2,"别名推送"),

    /**
     * web接入端
     */
    TERMINALTOWEB(1,"web端"),

    /**
     * 党建APP接入端
     */
    TERMINALTOPBAPP(2,"智慧党建APP"),

    /**
     * 志愿者APP接入端
     */
    TERMINALTOVOLAPP(3,"志愿者APP"),

    /**
     * 巡检APP接入端
     */
    TERMINALTOPIAPP(4,"巡检APP"),

    /**
     * 政务app接入端
     */
    TERMINALTOOAAPP(5,"政务app"),

    /**
     * 安检app接入端
     */
    TERMINALTOSVAPP(10,"安检app"),

    /**
     * 环保app接入端
     */
    TERMINALTOEPAPP(11,"环保app"),

    /**
     * 城建app接入端
     */
    TERMINALTOUCAPP(12,"城建app"),

    /**
     * 未读
     */
    UNREAD(0,"未读"),

    /**
     * 消息内容
     */
    MESSAGECONTENT(1,"content"),

    /**
     * 消息标题
     */
    MESSAGETITLE(2,"title"),

    /**
     * 内部用户
     */
    INTERNALUSER(1,"内部用户"),

    /**
     * 外部用户
     */
    EXTERNALUSER(2,"外部用户"),

    /**
     * 推送范围--所有用户
     */
    PUSHTOALLUSER(0,"所有用户"),

    /**
     * 推送范围--组织
     */
    PUSHTOORGANIZATION(2,"组织"),

    /**
     * 党建组织
     */
    PBORG(1,"党建组织"),

    /**
     * 政务组织
     */
    OADEP(2,"政务组织"),

    /**
     * 志愿者组织
     */
    VOLORG(8,"志愿者组织"),

    /**
     * 消息提醒
     */
    MESSAGE_REMIND(1,"msg_type"),

    /**
     * 待办提醒
     */
    UNDEAL_REMIND(2,"msg_type"),

    /**
     * 通知提醒
     */
    NOTICE_REMIND(3,"msg_type"),

    /**
     * 公告提醒
     */
    ANNOUNTCEMENT_REMIND(4,"msg_type"),

    /**
     * 已办提醒
     */
    DEAL_REMIND(5,"msg_type");

    /**
     * 操作码
     */
    private int code;

    /**
     * 操作信息
     */
    private String info;

    /**
     * @param code  状态码
     * @param info  状态信息
     */
    MqConstEnum(int code, String info) {
        this.code = code;
        this.info = info;
    }

    public int getCode() {
        return code;
    }


    public String getInfo() {
        return info;
    }
}


package org.tianlangstudio.data.hamal.common;

import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TaskCost implements Serializable {
    private static final long serialVersionUID = 1l;
    private Date beginTime;
    private Date endTime;
    private DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss SSS");
    public TaskCost() {

    }
    public TaskCost(Date beginTime) {
        this.beginTime = beginTime;
    }
    public TaskCost(Date beginTime, Date endTime) {
        this.endTime = endTime;
    }

    public String getBeginTime() {
        if(beginTime != null) {
            return dateFormat.format(beginTime);
        }
        return "";
    }
    public Date getBeginDateTime() {
        return this.beginTime;
    }
    public void setBeginTime(Date beginTime) {
        this.beginTime = beginTime;
    }

    public String getEndTime() {
        if(endTime != null) {
            return dateFormat.format(endTime);
        }

        return "";
    }
    public Date getEndDateTime() {
        return this.endTime;
    }
    public void setEndTime(Date endTime) {
        this.endTime = endTime;
    }
    public Long getCostMs() {
        if(endTime == null || beginTime == null) {
            return -1l;
        }
        return this.endTime.getTime() - this.beginTime.getTime();
    }
    public String getCost() {
        return getCostMs()/1000 + "s";
    }
}

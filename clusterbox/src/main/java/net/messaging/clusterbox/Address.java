package net.messaging.clusterbox;

import java.io.Serializable;

import com.fasterxml.jackson.databind.ObjectMapper;

public class Address implements Serializable {

    private static final long serialVersionUID = -1268837008453727844L;

    private String clusterBoxName;
    private String clusterBoxMailBoxName;
    private static ObjectMapper mapper = new ObjectMapper();

    public String getClusterBoxName() {
        return clusterBoxName;
    }

    public void setClusterBoxName(String clusterBoxName) {
        this.clusterBoxName = clusterBoxName;
    }

    public String getClusterBoxMailBoxName() {
        return clusterBoxMailBoxName;
    }

    public void setClusterBoxMailBoxName(String clusterBoxMailBoxName) {
        this.clusterBoxMailBoxName = clusterBoxMailBoxName;
    }

    public String toJson() {
        try {
            return mapper.writeValueAsString(this);
        } catch (Exception e) {
            return null;
        }
    }

    public static Address fromJson(String json) {
        try {
            return mapper.readValue(json, Address.class);
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public String toString() {
        return "Address [clusterBoxName=" + clusterBoxName + ", clusterBoxMailBoxName=" + clusterBoxMailBoxName + "]";
    }

}

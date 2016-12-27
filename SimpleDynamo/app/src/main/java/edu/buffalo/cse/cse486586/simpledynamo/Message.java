package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class Message implements Serializable {

    private static final long serialVersionUID = 7863261235394617847L;

    String type;
    String key;
    String value;
    String myPort;
    //String successor;
    //String predecessor;
    String query;
    Map<String, String> cursorList;

    public Message() {

    }

    public Message(String type, String key, String value) {
        this.type = type;
        this.key = key;
        this.value = value;
    }

    public Message(String myPort, String type ,String query, Map<String, String> cursorList) {
        this.myPort = myPort;
        this.type = type;
        this.query = query;
        this.cursorList = cursorList;
    }

    public Message(String myPort, String type ,String query, String key, String value) {
        this.myPort = myPort;
        this.type = type;
        this.query = query;
        this.key = key;
        this.value = value;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getMyPort() {
        return myPort;
    }

    public void setMyPort(String myPort) {
        this.myPort = myPort;
    }

    /*public String getSuccessor() {
        return successor;
    }

    public void setSuccessor(String successor) {
        this.successor = successor;
    }

    public String getPredecessor() {
        return predecessor;
    }

    public void setPredecessor(String predecessor) {
        this.predecessor = predecessor;
    }*/

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public Map<String, String> getCursorList() {
        return cursorList;
    }

    public void setCursorList(Map<String, String> cursorList) {
        this.cursorList = cursorList;
    }
}
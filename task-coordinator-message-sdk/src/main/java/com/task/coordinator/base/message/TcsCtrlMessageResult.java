package com.task.coordinator.base.message;

import java.io.IOException;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class TcsCtrlMessageResult<T> extends TcsCtrlMessage {

    private boolean isError;

    private String response;

    private Class<?> payLoadType;

    @JsonIgnore
    private final ObjectMapper mapper = new ObjectMapper();

    public TcsCtrlMessageResult(){

    }
    public TcsCtrlMessageResult(T response) {
        this.payLoadType = response.getClass();
        setResponse(response);
    }

    public TcsCtrlMessageResult(boolean isError, T response) {
        this.isError = isError;
        this.payLoadType = response.getClass();
        setResponse(response);
    }

    public T getResponse() {
        T content = null;
        try {
            content = (T) mapper.readValue(response, payLoadType);
        } catch (final IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return content;
    }

    public void setResponse(T response) {
        try {
            this.response = mapper.writeValueAsString(response);
        } catch (final JsonProcessingException e) {
            e.printStackTrace();
        }
    }

    public boolean isError() {
        return isError;
    }

    public void setError(boolean isError) {
        this.isError = isError;
    }

    public Class<?> getPayLoadType() {
        return payLoadType;
    }

    @Override
    public String toString() {
        return "TcsCtrlMessageResult [isError=" + isError + ", response=" + response + ", payLoadType=" + payLoadType
                + ", mapper=" + mapper + "]";
    }

}

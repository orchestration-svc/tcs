/* ****************************************************************************
 * Copyright (c) 2014 by Cisco Systems, Inc. All rights reserved.
 * ***************************************************************************/
package net.messaging.clusterbox.message;

import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import net.messaging.clusterbox.Address;

/**
 * Class that represents a mail sent between services and/or service instances
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public abstract class Message<T> {

    public final static String PUBLIC_NAMESPACE = "public";
    @JsonProperty
    protected Map<String, Map<String, String>> context;

    @JsonProperty
    protected Deque<Address> from;

    @JsonProperty
    protected Address to;

    private String requestKey;
    private T payload;

    public Message() {

    }

    public Message(T payload) {
        this.payload = payload;
    }

    public T getPayload() {
        return payload;
    }

    public void setPayload(T payload) {
        this.payload = payload;
    }

    public String getRequestKey() {
        return requestKey;
    }

    public void setRequestKey(String requestKey) {
        this.requestKey = requestKey;
    }

    /**
     * Returns the context for the specified key in the specified namespace
     *
     * @param namespace
     *            Namespace in which to look for the context
     * @param key
     *            Key for the context
     * @return String context if found, null otherwise
     */
    public String getContext(String namespace, String key) {
        String value = null;
        Map<String, String> namespaceMap = context.get(namespace);
        if (namespaceMap != null) {
            value = namespaceMap.get(key);
        }
        return value;
    }

    /**
     * Returns the context for the specified key in the "public" namespace
     *
     * @param key
     *            Key for the context
     * @return String context if found, null otherwise
     */
    public String getPublicContext(String key) {
        return getContext(PUBLIC_NAMESPACE, key);
    }

    /**
     * Adds context to the "public" namespace
     *
     * @param key
     *            Key under which to store the public context
     * @param value
     *            The public context to store
     */
    public void setPublicContext(String key, String value) {
        setContext(PUBLIC_NAMESPACE, key, value);
    }

    /**
     * Adds context to the specified namespace
     *
     * @param namespace
     *            Namespace in which to store the context
     * @param key
     *            Key under which to store the context
     * @param value
     *            The context to store
     */
    public void setContext(String namespace, String key, String value) {
        if (context == null) {
            context = new HashMap<String, Map<String, String>>();
        }

        Map<String, String> namespaceMap = context.get(namespace);
        if (namespaceMap == null) {
            namespaceMap = new HashMap<String, String>();
            context.put(namespace, namespaceMap);
        }

        namespaceMap.put(key, value);
    }

    /**
     * Removes context from the specified namespace
     *
     * @param namespace
     *            Namespace from which to remove the context
     * @param key
     *            Key of the context to remove
     */
    public void removeContext(String namespace, String key) {
        Map<String, String> namespaceMap = context.get(namespace);
        if (namespaceMap != null) {
            namespaceMap.remove(key);
        }
    }

    public Map<String, Map<String, String>> getContext() {
        return copyContext(context);
    }

    /**
     * Deletes all context for the specified namespace
     *
     * @param namespace
     *            Namespace to clear
     */
    public void clearContext(String namespace) {
        if (context != null) {
            context.remove(namespace);
        }
    }

    public Address peekFrom() {
        if (from != null) {
            return from.peek();
        }
        return null;
    }

    public Address pollFrom() {
        if (from != null) {
            return from.poll();
        }
        return null;
    }

    public Address getTo() {
        return to;
    }

    public void setTo(Address to) {
        this.to = to;
    }

    private Map<String, Map<String, String>> copyContext(Map<String, Map<String, String>> contextToCopy) {

        Map<String, Map<String, String>> contextToReturn = new HashMap<String, Map<String, String>>();
        if (contextToCopy != null) {
            for (Entry<String, Map<String, String>> entry : contextToCopy.entrySet()) {
                contextToReturn.put(entry.getKey(), new HashMap<String, String>(entry.getValue()));
            }
        }
        return contextToReturn;
    }

    @Override
    public String toString() {
        return "Message [context=" + context + ", from=" + from + ", to=" + to + ", requestKey=" + requestKey
                + ", payload=" + payload + "]";
    }

}

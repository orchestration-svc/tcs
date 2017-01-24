package net.messaging.clusterbox.message;

import java.util.LinkedList;

import net.messaging.clusterbox.Address;

public class RequestMessage<T> extends Message<T> {

    public void pushFrom(Address address) {
        if (from == null) {
            from = new LinkedList<Address>();

        }
        from.push(address);
    }

}

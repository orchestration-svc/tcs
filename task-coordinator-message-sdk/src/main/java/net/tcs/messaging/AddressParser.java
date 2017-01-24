package net.tcs.messaging;

import com.task.coordinator.endpoint.TcsTaskExecutionEndpoint;

public class AddressParser {
    /**
     *
     * @param uri
     *            of the form rmq://address/exchange/ROUTING_KEY
     * @return
     */
    public static TcsTaskExecutionEndpoint parseAddress(String uri) {
        String address = uri;
        if (uri.startsWith("rmq://")) {
            address = uri.substring(6);
        }

        final String[] addr = address.split("/");
        if (addr == null || addr.length < 3) {
            throw new IllegalArgumentException(uri);
        }

        return new TcsTaskExecutionEndpoint(addr[0], addr[1], addr[2]);
    }
}

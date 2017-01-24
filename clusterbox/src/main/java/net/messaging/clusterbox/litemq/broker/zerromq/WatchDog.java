package net.messaging.clusterbox.litemq.broker.zerromq;

import java.util.Comparator;
import java.util.PriorityQueue;

public class WatchDog {

    private static class Client {
        private Long timestamp;
        private Long threshold;

        protected Client(long timestamp, long threshold) {
            this.timestamp = timestamp;
            this.threshold = threshold;
        }

        @Override
        public String toString() {
            return "Client [timestamp=" + timestamp + ", threshold=" + threshold + "]";
        }

    }

    private static class MyComparator implements Comparator<Client> {

        @Override
        public int compare(Client o1, Client o2) {
            return o1.timestamp.compareTo(o1.timestamp);
        }
    }

    public static void main(String[] args) {
        MyComparator comparator = new MyComparator();
        PriorityQueue<Client> queue = new PriorityQueue<>(10, comparator);
        queue.add(new Client(1, 10));
        queue.add(new Client(5, 10));
        queue.add(new Client(3, 10));
        queue.add(new Client(2, 10));
        int y = 10;
        for (int i = 0; i < 4; i++) {
            System.out.println(queue.poll());
        }
    }

}

package framework;

import java.util.List;

public class Worker {
    private long superstep;

    /**
     * Master will invoke this to update superstep of each worker.
     * 
     * @param superstep new superstep.
     */
    void setSuperstep(long superstep) {
        this.superstep = superstep;
    }

    long getSuperstep() {
        return this.superstep;
    }

    /**
     * Vertices will invoke this to send message to other vertices.
     * 
     * @param message message to send.
     */
    void send(Message message) {
        
    }

    /**
     * Vertices will invoke this to receive messages from other vertices.
     * 
     * @param receiver receiver id.
     * @return list of messages
     */
    List<Message> receive(long receiver) {
        return null;
    }
}
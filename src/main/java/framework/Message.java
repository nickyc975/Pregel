package framework;

public abstract class Message {
    private long sender = -1;
    private long receiver = -1;
    private long timestamp = -1;
    private long superstep = -1;

    final Message setSender(long sender) {
        this.sender = sender;
        return this;
    }

    final Message setReceiver(long receiver) {
        this.receiver = receiver;
        return this;
    }

    final Message setTimestamp(long timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    final Message setSuperstep(long superstep) {
        this.superstep = superstep;
        return this;
    }

    final long getSender() {
        return this.sender;
    }

    final long getReceiver() {
        return this.receiver;
    }

    final long getTimestamp() {
        return this.timestamp;
    }

    final long getSuperstep() {
        return this.superstep;
    }
}
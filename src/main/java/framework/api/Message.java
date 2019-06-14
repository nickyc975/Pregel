package framework.api;

public abstract class Message {
    private long sender;
    private long receiver;
    private long superstep;

    public final long getSender() {
        return this.sender;
    }

    public final long getReceiver() {
        return this.receiver;
    }

    public final long getSuperstep() {
        return this.superstep;
    }

    final Message setSender(long sender) {
        this.sender = sender;
        return this;
    }

    final Message setReceiver(long receiver) {
        this.receiver = receiver;
        return this;
    }

    final Message setSuperstep(long superstep) {
        this.superstep = superstep;
        return this;
    }
}
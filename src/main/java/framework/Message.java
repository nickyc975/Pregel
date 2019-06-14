package framework;

final class Message<M> {
    private final M value;
    private long sender;
    private long receiver;
    private long superstep;

    Message(M value) {
        this.value = value;
    }

    final M getValue() {
        return this.value;
    }

    final long getSender() {
        return this.sender;
    }

    final long getReceiver() {
        return this.receiver;
    }

    final long getSuperstep() {
        return this.superstep;
    }

    final Message<M> setSender(long sender) {
        this.sender = sender;
        return this;
    }

    final Message<M> setReceiver(long receiver) {
        this.receiver = receiver;
        return this;
    }

    final Message<M> setSuperstep(long superstep) {
        this.superstep = superstep;
        return this;
    }
}
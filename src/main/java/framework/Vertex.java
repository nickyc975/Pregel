package framework;

import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

public abstract class Vertex {
    /**
     * The worker that this vertex belongs to. 
     */
    private final Worker context;

    /**
     * Indicate if this vertex is active.
     */
    private boolean isActive = true;

    /**
     * When current superstep is odd, use oddReceiveQueue to store 
     * the messages received in current superstep.
     */
    private final Queue<Message> oddReceiveQueue;

    /**
     * When current superstep is even, use evenReceiveQueue to store 
     * the messages received in current superstep.
     */
    private final Queue<Message> evenReceiveQueue;

    Vertex(Worker context) {
        this.context = context;
        this.oddReceiveQueue = new LinkedList<>();
        this.evenReceiveQueue = new LinkedList<>();
    }

    /**
     * Activate this vertex.
     */
    protected final void activate() {
        this.isActive = true;
    }

    /**
     * Check if this vertex is active.
     */
    protected final boolean isActive() {
        return this.isActive;
    }

    /**
     * Deactivate this vertex.
     */
    protected final void deactivate() {
        this.isActive = false;
    }

    /**
     * Send a message to the receiver.
     * 
     * @param receiver receiver id.
     * @param message message to send.
     */
    protected final void sendMessageTo(long receiver, Message message) {
        message.setSender(this.id())
               .setReceiver(receiver)
               .setSuperstep(context.getSuperstep());
        context.sendMessage(message);
    }

    /**
     * Check if this vertex has messages to deal with.
     * 
     * @return true if has messages, false otherwise.
     */
    protected final boolean hasMessages() {
        if (context.getSuperstep() % 2 == 0) {
            return !oddReceiveQueue.isEmpty();
        } else {
            return !evenReceiveQueue.isEmpty();
        }
    }

    /**
     * Read a message sent to this vertex for computing.
     * 
     * @return message received.
     */
    protected final Message readMessage() {
        if (context.getSuperstep() % 2 == 0) {
            return oddReceiveQueue.remove();
        } else {
            return evenReceiveQueue.remove();
        }
    }

    /**
     * Send a message to all neighbors.
     * 
     * @param message message to be sent.
     */
    protected final void sendMessage(Message message) {
        for (Long target : getOuterEdges().keySet()) {
            sendMessageTo(target, message);
        }
    }

    /**
     * Worker will invoke this method to deliver messages to vertices.
     * 
     * @param message message sent to this vertex.
     */
    protected final void receiveMessage(Message message) {
        if (context.getSuperstep() % 2 == 0) {
            evenReceiveQueue.add(message);
        } else {
            oddReceiveQueue.add(message);
        }
    }

    /**
     * Get id of this vertex.
     * 
     * Id must be globally identical.
     * 
     * @return id of this vertex.
     */
    public abstract long id();

    /**
     * The main calculating logic of each vertex.
     */
    public abstract void compute();

    /**
     * Parse user defined properties of a vertex object from string.
     * 
     * @param str string, usually from toString() of this class.
     */
    public abstract void fromString(String str);

    /**
     * Get outer edges of this vertex.
     * 
     * @return outer edges of this vertex.
     */
    public abstract Map<Long, ? extends Edge> getOuterEdges();
}
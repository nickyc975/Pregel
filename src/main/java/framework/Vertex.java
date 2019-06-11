package framework;

import java.util.List;

public abstract class Vertex {
    /**
     * The worker that this vertex belongs to. 
     */
    private Worker context;

    /**
     * Indicate if this vertex is active.
     */
    private boolean isActive = true;

    Vertex(Worker context) {
        this.context = context;
    }

    final void setContext(Worker context) {
        this.context = context;
    }

    /**
     * Send a message to the receiver.
     * 
     * @param receiver receiver id.
     * @param message message to send.
     */
    protected final void sendMessage(long receiver, Message message) {
        message.setSender(this.id())
               .setReceiver(receiver)
               .setTimestamp(System.currentTimeMillis())
               .setSuperstep(context.getSuperstep());
        context.send(message);
    }

    /**
     * Read message sent to this vertex for computing.
     * 
     * @return list of messages.
     */
    protected final List<Message> readMessages() {
        return context.receive(this.id());
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
     * @param str
     */
    public abstract void fromString(String str);
}
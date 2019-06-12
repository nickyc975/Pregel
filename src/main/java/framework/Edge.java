package framework;

public abstract class Edge {
    /**
     * Get the id of the source of this outer edge.
     * 
     * @return id of source.
     */
    public abstract long getSource();

    /**
     * Get the id of the target of this outer edge.
     * 
     * @return id of target.
     */
    public abstract long getTarget();
}
package framework;

public abstract class Edge {
    private final long source;

    private final long target;

    Edge(long source, long target) {
        this.source = source;
        this.target = target;
    }

    /**
     * Get the id of the source of this outer edge.
     * 
     * @return id of source.
     */
    public long getSource() {
        return this.source;
    }

    /**
     * Get the id of the target of this outer edge.
     * 
     * @return id of target.
     */
    public long getTarget() {
        return this.target;
    }

    /**
     * Parse user defined properties of an edge object from strings.
     * 
     * @param strings Every string is a property. The first two strings are source id and target id.
     */
    public abstract void fromStrings(String[] strings);
}
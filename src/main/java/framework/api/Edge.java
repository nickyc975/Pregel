package framework.api;

public abstract class Edge {
    /**
     * Source vertex id.
     */
    private long source;

    /**
     * target vertex id.
     */
    private long target;

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

    /**
     * Create instance of subclass of Edge.
     * 
     * @param klass subclass of Edge.
     * @param source source vertex id.
     * @param target target vertex id.
     * @return instance of the subclass.
     */
    public static final Edge newInstance(Class<? extends Edge> klass, long source, long target) {
        try {
            Edge instance = klass.newInstance();
            instance.source = source;
            instance.target = target;
            return instance;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
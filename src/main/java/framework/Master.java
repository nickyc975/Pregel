package framework;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

public class Master {
    /**
     * Current superstep.
     */
    private long superstep = 0;

    /**
     * Workers registered on this master.
     */
    private final Map<Long, Worker> workers;

    /**
     * The partitions information.
     */
    private final Map<Long, Set<Long>> partitions;

    public Master() {
        workers = new HashMap<>();
        partitions = new HashMap<>();
    }

    /**
     * Generate id for workers.
     * 
     * @return an identical id.
     */
    long generateId() {
        return 0;
    }

    long getSuperstep() {
        return this.superstep;
    }

    /**
     * Forward message between workers.
     * 
     * @param message message to be sent.
     */
    void sendMessage(Message message) {
        long receiver = message.getReceiver();
        for (Entry<Long, Set<Long>> entry : partitions.entrySet()) {
            long key = entry.getKey();
            Set<Long> value = entry.getValue();
            if (value.contains(receiver)) {
                workers.get(key).receiveMessage(message);
                return;
            }
        }
    }
}

package algorithm;

import framework.Master;
import framework.api.Edge;
import framework.api.Message;
import framework.api.Vertex;

public class PageRank {
    public static void main(String[] args) {
        Master master = new Master().setEdgeClass(PageRankEdge.class)
                                    .setVertexClass(PageRankVertex.class)
                                    .setNumPartitions(4)
                                    .setWorkPath("data/page_rank");
        master.loadGraph("data/web-Google.txt");
        master.run();
    }

    public static class PageRankVertex extends Vertex {
        private double value;
    
        @Override
        public void compute() {
            if (context.getSuperstep() >= 1) {
                double sum = 0;
                while (hasMessages()) {
                    sum += ((PageRankMessage)readMessage()).getValue();
                    value = 0.15 / context.getTotalNumVertices() + 0.85 * sum;
                }
            }
    
            if (context.getSuperstep() < 30) {
                int n = getOuterEdges().size();
                sendMessage(new PageRankMessage(value / n));
            }
        }
    
        @Override
        public void fromStrings(String[] strings) {
            
        }
    }
    
    public static class PageRankEdge extends Edge {
        @Override
        public void fromStrings(String[] strings) {
    
        }
    }
    
    public static class PageRankMessage extends Message {
        private final double value;
    
        PageRankMessage(double value) {
            this.value = value;
        }
    
        double getValue() {
            return this.value;
        }
    }
}
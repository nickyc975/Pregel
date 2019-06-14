package algorithm;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;

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
        Iterator<Vertex> vertices = master.getVertices();

        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter("data/page_rank/output.txt"));
            while (vertices.hasNext()) {
                PageRankVertex vertex = (PageRankVertex) (vertices.next());
                String output = String.format("%d\t%f\n", vertex.id(), vertex.getValue());
                writer.write(output);
            }
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static class PageRankVertex extends Vertex {
        private double value;

        public double getValue() {
            return value;
        }
    
        @Override
        public void compute() {
            if (context.getSuperstep() >= 1) {
                double sum = 0;
                while (hasMessages()) {
                    sum += ((PageRankMessage)readMessage()).getValue();
                }
                value = 0.15 / context.getTotalNumVertices() + 0.85 * sum;
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
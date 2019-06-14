package algorithm;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;
import java.util.function.Consumer;

import framework.Master;
import framework.Vertex;
import framework.api.EdgeValue;
import framework.api.VertexValue;

public class PageRank {
    public static void main(String[] args) {
        Master<DoubleValue, EmptyValue, Double> master = new Master<DoubleValue, EmptyValue, Double>();
        master.setNumPartitions(4)
              .setWorkPath("data/page_rank")
              .setEdgeParser(s -> {
                  String[] parts = s.split("\t");
                  return new EmptyValue(Long.parseLong(parts[0]), Long.parseLong(parts[1]));
              }).setVertexParser(s -> {
                  String[] parts = s.split("\t");
                  return new DoubleValue(Long.parseLong(parts[0]));
              });

        Consumer<Vertex<DoubleValue, EmptyValue, Double>> computeFunction = vertex -> {
            DoubleValue value = vertex.getValue();
            if (value == null) {
                value = new DoubleValue(vertex.id());
                value.setValue(0);
                vertex.setValue(value);
            }
            if (vertex.context().getSuperstep() >= 1) {
                double sum = 0;
                while (vertex.hasMessages()) {
                    sum += vertex.readMessage();
                }
                value.setValue(0.15 / vertex.context().getTotalNumVertices() + 0.85 * sum);
            }
    
            if (vertex.context().getSuperstep() < 30) {
                int n = vertex.getOuterEdges().size();
                vertex.sendMessage(value.getValue() / n);
            }
        };

        master.setComputeFunction(computeFunction);
        master.loadEdges("data/web-Google.txt");
        master.run();

        Iterator<Vertex<DoubleValue, EmptyValue, Double>> vertices = master.getVertices();
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter("data/page_rank/output.txt"));
            while (vertices.hasNext()) {
                DoubleValue value = vertices.next().getValue();
                String output = String.format("%d\t%f\n", value.id(), value.getValue());
                writer.write(output);
            }
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static class DoubleValue extends VertexValue {
        private final long id;
        private double value = 0;

        public DoubleValue(long id) {
            this.id = id;
        }

        @Override
        public long id() {
            return this.id;
        }

        public double getValue() {
            return value;
        }

        public void setValue(double value) {
            this.value = value;
        }
    }
    
    public static class EmptyValue extends EdgeValue {
        private long source;
        private long target;

        public EmptyValue(long source, long target) {
            this.source = source;
            this.target = target;
        }

        @Override
        public long source() {
            return this.source;
        }

        @Override
        public long target() {
            return this.target;
        }
    }
}
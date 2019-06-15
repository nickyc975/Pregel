package algorithm;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;
import java.util.function.Consumer;

import framework.Master;
import framework.Vertex;
import framework.utils.Tuple2;
import framework.utils.Tuple3;

public class PageRank {
    public static void main(String[] args) {
        Master<Double, Void, Double> master = new Master<Double, Void, Double>();
        master.setNumPartitions(4)
              .setWorkPath("data/page_rank")
              .setEdgeParser(s -> {
                  String[] parts = s.split("\t");
                  return new Tuple3<>(Long.parseLong(parts[0]), Long.parseLong(parts[1]), null);
              }).setVertexParser(s -> {
                  String[] parts = s.split("\t");
                  return new Tuple2<>(Long.parseLong(parts[0]), 0.0);
              });

        Consumer<Vertex<Double, Void, Double>> computeFunction = vertex -> {
            if (vertex.getValue() == null) {
                vertex.setValue(0.0);
            }

            if (vertex.hasMessages()) {
                double sum = 0;
                while (vertex.hasMessages()) {
                    sum += vertex.readMessage();
                }
                double value = 0.15 / vertex.context().getTotalNumVertices() + 0.85 * sum;
                vertex.setValue(value);
            }

            int n = vertex.getOuterEdges().size();
            vertex.sendMessage(vertex.getValue() / n);
    
            if (vertex.context().getSuperstep() > 30) {
                vertex.voteToHalt();
            }
        };

        master.setComputeFunction(computeFunction);
        master.loadEdges("data/web-Google.txt");
        master.run();

        Iterator<Vertex<Double, Void, Double>> vertices = master.getVertices();
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter("data/page_rank/output.txt"));
            while (vertices.hasNext()) {
                Vertex<Double, Void, Double> vertex = vertices.next();
                String output = String.format("%d\t%f\n", vertex.id(), vertex.getValue());
                writer.write(output);
            }
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
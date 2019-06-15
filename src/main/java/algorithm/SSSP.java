package algorithm;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;
import java.util.function.Consumer;

import framework.Combiner;
import framework.Master;
import framework.Vertex;
import framework.utils.Tuple2;
import framework.utils.Tuple3;

public class SSSP {
    public static void main(String[] args) {
        Master<Double, Double, Double> master = new Master<Double, Double, Double>();
        master.setNumPartitions(4)
              .setWorkPath("data/sssp")
              .setEdgeParser(s -> {
                  String[] parts = s.split("\t");
                  return new Tuple3<>(Long.parseLong(parts[0]), Long.parseLong(parts[1]), 1.0);
              }).setVertexParser(s -> {
                  String[] parts = s.split("\t");
                  return new Tuple2<>(Long.parseLong(parts[0]), 0.0);
              }).setCombiner(new Combiner<Double>(){
                  @Override
                  public Double combine(Double a, Double b) {
                      return Double.min(a, b);
                  }
              });

        Consumer<Vertex<Double, Double, Double>> computeFunction = vertex -> {
            double minValue = vertex.id() == 0 ? 0 : Double.POSITIVE_INFINITY;

            if (vertex.context().getSuperstep() == 0) {
                vertex.setValue(minValue);
            } else {
                double originalValue = vertex.getValue();

                while(vertex.hasMessages()) {
                    minValue = Double.min(minValue, vertex.readMessage());
                    if (minValue < vertex.getValue()) {
                        vertex.setValue(minValue);
                    }
                }

                if (minValue >= originalValue) {
                    vertex.voteToHalt();
                    return;
                }
            }

            for (Tuple3<Long, Long, Double> edge : vertex.getOuterEdges().values()) {
                vertex.sendMessageTo(edge._2, edge._3 + minValue);
            }
        };

        master.setComputeFunction(computeFunction);
        master.loadEdges("data/web-Google.txt");
        master.run();

        Iterator<Vertex<Double, Double, Double>> vertices = master.getVertices();
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter("data/sssp/output.txt"));
            while (vertices.hasNext()) {
                Vertex<Double, Double, Double> vertex = vertices.next();
                String output = String.format("%d\t%f\n", vertex.id(), vertex.getValue());
                writer.write(output);
            }
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
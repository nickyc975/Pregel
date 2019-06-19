package examples;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;
import java.util.function.Consumer;

import pregel.Aggregator;
import pregel.Combiner;
import pregel.Master;
import pregel.Vertex;
import pregel.utils.Tuple2;
import pregel.utils.Tuple3;

public class PageRank {
    public static void main(String[] args) {
        Master<Double, Void, Double> master = new Master<Double, Void, Double>(4, "data/page_rank");
        master.setEdgeParser(s -> {
                  String[] parts = s.split("\t");
                  return new Tuple3<>(Long.parseLong(parts[0]), Long.parseLong(parts[1]), null);
              }).setVertexParser(s -> {
                  String[] parts = s.split("\t");
                  return new Tuple2<>(Long.parseLong(parts[0]), 0.0);
              }).setCombiner(new Combiner<Double>(){
                  @Override
                  public Double combine(Double a, Double b) {
                      return a + b;
                  }
              }).addAggregator("maxVertex", 
                  new Aggregator<Vertex<Double, Void, Double>, Tuple2<Long, Double>>() {
                      @Override
                      public Tuple2<Long, Double> report(Vertex<Double, Void, Double> vertex) {
                          return new Tuple2<>(vertex.id(), vertex.getValue());
                      }

                      @Override
                      public Tuple2<Long, Double> aggregate(Tuple2<Long, Double> a, Tuple2<Long, Double> b) {
                          return a._2 > b._2 ? a : b;
                      }
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
                double value = 0.15 / vertex.context().getNumVertices() + 0.85 * sum;
                vertex.setValue(value);
            }

            int n = vertex.getOuterEdges().size();
            vertex.sendMessage(vertex.getValue() / n);
    
            if (vertex.context().superstep() > 30) {
                vertex.voteToHalt();
            }
        };

        master.setComputeFunction(computeFunction);
        master.loadEdges("data/web-Google.txt");
        master.run();

        Tuple2<Long, Double> result = master.getAggregatedValue("maxVertex");
        System.out.println("Max vertex id: " + result._1 + ", weight: " + result._2);

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
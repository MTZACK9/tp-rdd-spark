package ma.enset;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("main");
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            JavaRDD<String> rddLines = sc.textFile("ventes.txt");
            JavaPairRDD<String, Double> ventesParVille = rddLines.mapToPair(line -> {
               String[] parts = line.split(" ");
               String ville = parts[1];
               double prix = Double.parseDouble(parts[3]);
               return new Tuple2<>(ville, prix);
            });

            JavaPairRDD<String, Double> totalVentesParVille = ventesParVille.reduceByKey((v1, v2) -> v1 + v2);
            totalVentesParVille.collect().forEach(t -> System.out.println(t._1() +" "+ t._2()));
        }
    }
}
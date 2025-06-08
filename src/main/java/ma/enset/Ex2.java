package ma.enset;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class Ex2 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("VentesParVilleEtAnnee");

        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            JavaRDD<String> rddLines = sc.textFile("ventes.txt");

            JavaPairRDD<String, Double> ventesParVilleEtAnnee = rddLines.mapToPair(line -> {
                String[] parts = line.split(" ");
                String date = parts[0];
                String ville = parts[1];
                double prix = Double.parseDouble(parts[3]);

                String annee = date.split("-")[0];

                String cle = ville + "_" + annee;

                return new Tuple2<>(cle, prix);
            });

            JavaPairRDD<String, Double> totalVentesParVilleEtAnnee = ventesParVilleEtAnnee.reduceByKey((v1, v2) -> v1 + v2);

            totalVentesParVilleEtAnnee.collect().forEach(t -> {
                String[] parts = t._1().split("_");
                String ville = parts[0];
                String annee = parts[1];
                System.out.println("Ville: " + ville + " | Année: " + annee + " | Total: " + t._2());
            });
        }
    }
}

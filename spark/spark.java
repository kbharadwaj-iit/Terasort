import scala.Tuple2;
import java.io.*;
import org.apache.spark.api.java.JavaPairRDD;
import java.util.ArrayList;
import org.apache.spark.api.java.JavaRDD;
import java.util.List;
import org.apache.spark.api.java.function.FlatMapFunction;
import java.util.*;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.JavaSparkContext;

public class spark {
    
    	private static class KeyPairSeperator implements PairFunction<String, String, String> {
	public Tuple2<String, String> call(String s) {
			return new Tuple2<String, String>(s.substring(0, 10), s.substring(10));
		}
	}

	public static void main(String[] args) {
	
		Long sortTime;
		@SuppressWarnings("resource")
        Long startTime = System.currentTimeMillis();
		JavaSparkContext sc ;
        sc = new JavaSparkContext("local[*]", "spark sort");
		Long progStartTime ;
        progStartTime = System.currentTimeMillis();
		JavaRDD<String> lines;
        lines = sc.textFile(args[0]);
		JavaPairRDD<String, String> output;
        output = lines.mapToPair(new KeyPairSeperator()).sortByKey(true);
         output.flatMap(new FlatMapFunction<Tuple2<String, String>, String>() {
			

			public Iterator<String> call(Tuple2<String, String> t) throws Exception {
				ArrayList<String> lst = new ArrayList<String>();
                String s1 = t._1();
                String s2 = t._2().trim();
				lst.add(s1 + "  " + s2 + "\r");
				return lst.iterator();
			}
		}).saveAsTextFile(args[1]);
		sortTime = System.currentTimeMillis() - startTime;
		System.out.println( "Total Sort time: "+sortTime/1000 + "s");
	}

	

}
package okkam.it.flink.flink150SNAPSHOT;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.ReduceOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

public class FlinkTest {

  public static void main(String[] args) throws Exception {
    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    DataSet<String> lines = env.readTextFile("test.csv");

    DataSet<Tuple2<String, Integer>> wordCounts1 =
        lines.flatMap(new LineSplitter()).groupBy(0).sum(1);

    DataSet<Tuple2<String, Integer>> wordCounts2 =
        lines.flatMap(new LineSplitter2()).groupBy(0).sum(1);

    DataSet<Tuple2<String, Integer>> fullCounts = wordCounts1.union(wordCounts2).groupBy(0)
        .reduce((t1, t2) -> new Tuple2<>(t1.f0, t1.f1 + t2.f1));

    DataSet<Tuple2<String, Integer>> moreThan10 = fullCounts.filter(t -> t.f1 > 10);
    DataSet<Tuple2<String, Integer>> lessThan2 = fullCounts.filter(t -> t.f1 < 2);

    DataSet<Pojo> aa =
        moreThan10.map(t -> new Pojo(String.valueOf(t.f0.charAt(4)))).returns(Pojo.class);
    DataSet<Pojo> bb =
        lessThan2.map(t -> new Pojo(String.valueOf(t.f0.charAt(4)))).returns(Pojo.class);

    DataSet<Pojo> full = aa.union(bb);

    ReduceOperator<Pojo> end = full.reduce((d, c) -> new Pojo(d.name + "-" + c.name));

    end.writeAsText("out.txt");

    JobExecutionResult jobInfo = env.execute("ES indexing");
    System.out.printf("Job took %s minutes", jobInfo.getNetRuntime(TimeUnit.MINUTES));
  }

  public static class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
    private static final long serialVersionUID = 1L;

    @Override
    public void flatMap(String line, Collector<Tuple2<String, Integer>> out) {
      for (String word : line.split(",")) {
        out.collect(new Tuple2<String, Integer>(word.substring(0, 6), 1));
      }
    }
  }

  public static class LineSplitter2 implements FlatMapFunction<String, Tuple2<String, Integer>> {
    private static final long serialVersionUID = 1L;

    @Override
    public void flatMap(String line, Collector<Tuple2<String, Integer>> out) {
      for (String word : line.split(",")) {
        out.collect(new Tuple2<String, Integer>(word.substring(1, 7), 1));
      }
    }
  }

}

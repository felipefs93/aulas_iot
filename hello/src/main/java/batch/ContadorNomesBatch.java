package batch;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

public class ContadorNomesBatch {

	public static void main(String[] args) throws Exception {
		
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		ParameterTool params = ParameterTool.fromArgs(args);
		
		env.getConfig().setGlobalJobParameters(params);
		
		DataSet<String> text = env.readTextFile(params.get("input"));
		
		DataSet<String> filtrado = text.filter(new FilterFunction<String>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public boolean filter(String value) throws Exception {
				return value.startsWith("A");
			}
			
		});
		
		DataSet<Tuple2<String, Integer>> mapeado = filtrado.map(new Tokenizer());
		
		DataSet<Tuple2<String, Integer>> somado = mapeado.groupBy(new int[] {0}).sum(1);
		somado.writeAsCsv(params.get("output"), "\n", " ");
		
		env.execute("Hello 1 - Batch!");

	}
	
	public static final class Tokenizer implements MapFunction<String, Tuple2<String, Integer>>{

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		public Tuple2<String, Integer> map(String value) throws Exception {
			return new Tuple2<String, Integer>(value, Integer.valueOf(1));
		}
		
	}

}

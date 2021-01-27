package stream;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ContadorNomesStream {

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		
		DataStream<String> text = null;
		text = env.socketTextStream("localhost", 9999, "\n");
		
		DataStream<Tuple2<String, Integer>> contagem = text.map(new Tokenizer()).keyBy(value -> value.f0).sum(1);
		
		System.out.println("Fez todas as operacoes com o stream...");
		contagem.print();
		env.execute("Hello 2 - Stream!");
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

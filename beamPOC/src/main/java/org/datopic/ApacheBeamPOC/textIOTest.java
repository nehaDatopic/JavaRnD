package org.datopic.ApacheBeamPOC;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;


public class textIOTest {
	public static final void main(String args[]) throws Exception {
		
		System.out.println("In the start");


	    Pipeline pipeline = Pipeline.create();

	    pipeline
	        .apply(TextIO.read().from("/home/neha/eclipse_ws/input/testInput.txt"))
	        .apply("ParseAndConvertToKV", MapElements.via(new SimpleFunction<String, KV<String, Integer>>() {
	          @Override
	          public KV<String, Integer> apply(String input) {
	            String[] split = input.split(",");
	            if (split.length < 4) {
	              return null;
	            }
	            String key = split[1];
	            Integer value = Integer.valueOf(split[3]);
	            return KV.of(key, value);
	          }
	        }))
	        .apply(GroupByKey.<String, Integer>create())
	        .apply("SumUpValuesByKey", ParDo.of(new DoFn<KV<String, Iterable<Integer>>, String>() {
	          @ProcessElement
	          public void processElement(ProcessContext context) {
	            Integer totalSells = 0;
	            String brand = context.element().getKey();
	            Iterable<Integer> sells = context.element().getValue();
	            for (Integer amount : sells) {
	              totalSells += amount;
	            }
	            context.output(brand + ": " + totalSells);
	          }
	        }))
	        .apply(TextIO.write().to("/home/neha/eclipse_ws/output/output").withoutSharding());

	    pipeline.run();
	  }
}
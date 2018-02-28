package kr.co.bq.dataflow;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.junit.Test;

public class TestDataflowGrep {

	@Test
	public void test() {
		PipelineOptions options = PipelineOptionsFactory.fromArgs("").withValidation().create();
//		Pipeline p = Pipeline.create(options);
//
//		String input = "src/main/java/kr/co/bq/dataflow/*.java";
//		String outputPrefix = "/tmp/output";
//		final String searchTerm = "import";
//
//		p //
//				.apply("GetJava", TextIO.read().from(input)) //
//				.apply("Grep", ParDo.of(new DoFn<String, String>() {
//					@ProcessElement
//					public void processElement(ProcessContext c) throws Exception {
//						String line = c.element();
//						if (line.contains(searchTerm)) {
//							c.output(line);
//						}
//					}
//				})) //
//				.apply(TextIO.write().to(outputPrefix).withSuffix(".txt").withoutSharding());
//
//		p.run();
	}

}

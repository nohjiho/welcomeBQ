/*
 * Copyright (C) 2016 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package kr.co.bq.dataflow;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

/**
 * A dataflow pipeline that prints the lines that match a specific search term
 * 
 * @author vlakshmanan
 *
 */
/**
 * input 경로의 JAVA 파일을 라인별로 Read하여 searchTerm의 문자열을 포함하는 라인을 output.txt 파일에 저장.
 * @author 웰컴저축은행
 *
 */
public class Grep {

	@SuppressWarnings("serial")
	public static void main(String[] args) {
		System.out.println("Hello Dataflow!!");
		PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();
		Pipeline p = Pipeline.create(options);
		//TODO "*" 때문에 오류가 발생하는데 원인은?
		//String input = "src/main/java/kr/co/bq/dataflow/*.java";	
		String input = "src/main/java/kr/co/bq/dataflow/Grep.java";
		String outputPrefix = "/tmp/output";
		final String searchTerm = "import";

		p.apply("GetJava", TextIO.read().from(input))
			.apply("Grep", ParDo.of(new DoFn<String, String>() {
				@ProcessElement
				public void processElement(ProcessContext c) throws Exception {
					String line = c.element();
					if (line.contains(searchTerm)) {
						c.output(line);
					}
				}
			})) //
			.apply(TextIO.write().to(outputPrefix).withSuffix(".txt").withoutSharding());

		p.run();
	}
}

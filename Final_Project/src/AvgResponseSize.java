/*
 * Copyright (C) 2015 Google Inc.
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

package my.java.packageid;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.options.Validation;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.Mean;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;

import java.util.ArrayList;
import java.util.List;

/**
 * An example that reads the public samples of http archive data from BigQuery, and finds
 * the average response size ('resp_content_length') for each content type ('type').
 *
 * <p>Note: Before running this example, you must create a BigQuery dataset to contain your output
 * table.
 *
 * <p>To execute this pipeline locally, specify general pipeline configuration:
 * <pre>{@code
 *   --project=YOUR_PROJECT_ID
 * }
 * </pre>
 * and the BigQuery table for the output, with the form
 * <pre>{@code
 *   --output=YOUR_PROJECT_ID:DATASET_ID.TABLE_ID
 * }</pre>
 *
 * <p>To execute this pipeline using the Dataflow service, specify pipeline configuration:
 * <pre>{@code
 *   --project=YOUR_PROJECT_ID
 *   --stagingLocation=gs://YOUR_STAGING_DIRECTORY
 *   --runner=BlockingDataflowPipelineRunner
 * }
 * </pre>
 * and the BigQuery table for the output:
 * <pre>{@code
 *   --output=YOUR_PROJECT_ID:DATASET_ID.TABLE_ID
 * }</pre>
 *
 * <p>The BigQuery input table defaults to {@code httparchive:runs.2016_04_15_requests}
 * and can be overridden with {@code --input}.
 */
public class AvgResponseSize {
	// Default data set: httparchive:runs.2016_04_15_requests
	private static final String HTTP_ARCHIVE_TABLE =
			"httparchive:runs.2016_04_15_requests";
			// "gleaming-entry-130201:hqiu_httparchive_result.2016_04_15_selected_resp";
	
	private static final String AVG_RESPONSE_SIZE_TABLE =
			"gleaming-entry-130201:hqiu_httparchive_result.AvgResponseSize";

	/**
	 * Examines each row (http requests reading) in the input table. Output the response content
	 * length ('resp_content_length') and the response type ('type').
	 */
	static class ExtractLengthFn extends DoFn<TableRow, KV<String, Integer>> {
		@Override
		public void processElement(ProcessContext c) {
			TableRow row = c.element();
			String type = (String) row.get("type");
			String length = (String) row.get("resp_content_length");
			
			// Escape invalid content length
			if (length.contains(";") || length.contains(" ") || length.contains(",")
					|| (length.length() >= 10 && length.charAt(0) > '1')) {
				return;
			}
			
			if (type != null && ! type.isEmpty() && length != null && !length.isEmpty()) {
				Integer contentlen = Integer.parseInt(length);
				c.output(KV.of(type, contentlen));
			}
		}
	}

	/**
	 * Format the results to a TableRow, to save to BigQuery.
	 *
	 */
	static class FormatMeanFn extends DoFn<KV<String, Double>, TableRow> {
		@Override
		public void processElement(ProcessContext c) {
			TableRow row = new TableRow()
					.set("type", c.element().getKey())
					.set("avg_content_length", c.element().getValue() / 1024);
			c.output(row);
		}
	}

	/**
	 * Reads rows from http archive table, and finds the average resp_content_length for each
	 * type via the 'Mean' statistical combination function.
	 */
	static class MeanContentLength
	extends PTransform<PCollection<TableRow>, PCollection<TableRow>> {
		@Override
		public PCollection<TableRow> apply(PCollection<TableRow> rows) {

			// row... => <type, resp_content_length> ...
			PCollection<KV<String, Integer>> length = rows.apply(
					ParDo.of(new ExtractLengthFn()));

			// type, resp_content_length... => <type, mean resp_content_length>...
			PCollection<KV<String, Double>> meanLength =
					length.apply(Mean.<String, Integer>perKey());

			// <type, mean>... => row...
			PCollection<TableRow> results = meanLength.apply(
					ParDo.of(new FormatMeanFn()));

			return results;
		}
	}

	/**
	 * Options supported by {@link AvgResponseSize}.
	 *
	 * <p>Inherits standard configuration options.
	 */
	private static interface Options extends PipelineOptions {
		@Description("Table to read from, specified as "
				+ "<project_id>:<dataset_id>.<table_id>")
		@Default.String(HTTP_ARCHIVE_TABLE)
		String getInput();
		void setInput(String value);

		@Description("Table to write to, specified as "
				+ "<project_id>:<dataset_id>.<table_id>")
		@Default.String(AVG_RESPONSE_SIZE_TABLE)
		@Validation.Required
		String getOutput();
		void setOutput(String value);
	}

	public static void main(String[] args)
			throws Exception {

		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
		Pipeline p = Pipeline.create(options);

		// Build the table schema for the output table.
		List<TableFieldSchema> fields = new ArrayList<>();
		fields.add(new TableFieldSchema().setName("type").setType("STRING"));
		fields.add(new TableFieldSchema().setName("avg_content_length").setType("FLOAT"));
		TableSchema schema = new TableSchema().setFields(fields);

		p.apply(BigQueryIO.Read.from(options.getInput()))
		.apply(new MeanContentLength())
		.apply(BigQueryIO.Write
				.to(options.getOutput())
				.withSchema(schema)
				.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
				.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));

		p.run();
	}
}
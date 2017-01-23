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
import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;

import java.util.ArrayList;
import java.util.List;

/**
 * An example that reads the public samples of http archive data from BigQuery, analyzes number
 * of requests per page, and writes the results to BigQuery.
 *
 * <p>Concepts: Reading/writing BigQuery; counting a PCollection; user-defined PTransforms
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
public class RqstsPerPage {
	// Default data set: httparchive:runs.2016_04_15_requests
	private static final String HTTP_ARCHIVE_TABLE =
			"httparchive:runs.2016_04_15_requests";
			// "gleaming-entry-130201:hqiu_httparchive_result.2016_04_15_requests_sample";

	private static final String RQST_PER_PAGE_TABLE =
			"gleaming-entry-130201:hqiu_httparchive_result.RqstsPerPage";
	
	/**
	 * Examines each row in the input table. If a page id is recorded
	 * in that sample, the name of page id is the output.
	 */
	static class ExtractPageidFn extends DoFn<TableRow, String> {
		@Override
		public void processElement(ProcessContext c){
			TableRow row = c.element();
			c.output((String) row.get("pageid"));
		}
	}

	/**
	 * Prepares the data for writing to BigQuery by building a TableRow object containing the
	 * page id and the occurrences of the page id.
	 */
	static class FormatCountsFn extends DoFn<KV<String, Long>, TableRow> {
		@Override
		public void processElement(ProcessContext c) {
			TableRow row = new TableRow()
					.set("pageid", c.element().getKey())
					.set("rqsts_per_page", c.element().getValue());
			c.output(row);
		}
	}

	/**
	 * Takes rows from a table and generates a table of counts.
	 *
	 * The output contains the total number of requests found in each page id in
	 * the following schema:
	 * <ul>
	 *   <li>pageid: string</li>
	 *   <li>number: integer</li>
	 * </ul>
	 */
	static class CountRqstsPerPage
	extends PTransform<PCollection<TableRow>, PCollection<TableRow>> {
		@Override
		public PCollection<TableRow> apply(PCollection<TableRow> rows) {

			// row... => page id...
			PCollection<String> page = rows.apply(
					ParDo.of(new ExtractPageidFn()));

			// page id... => <page id,count>...
			PCollection<KV<String, Long>> pageCounts =
					page.apply(Count.<String>perElement());

			// <page id,count>... => row...
			PCollection<TableRow> results = pageCounts.apply(
					ParDo.of(new FormatCountsFn()));

			return results;
		}
	}

	/**
	 * Options supported by {@link RqstsPerPage}.
	 *
	 * <p>Inherits standard configuration options.
	 */
	private static interface Options extends PipelineOptions {
		@Description("Table to read from, specified as "
				+ "<project_id>:<dataset_id>.<table_id>")
		@Default.String(HTTP_ARCHIVE_TABLE)
		String getInput();
		void setInput(String value);

		@Description("BigQuery table to write to, specified as "
				+ "<project_id>:<dataset_id>.<table_id>. The dataset must already exist.")
		@Default.String(RQST_PER_PAGE_TABLE)
		@Validation.Required
		String getOutput();
		void setOutput(String value);
	}

	public static void main(String[] args) {
		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

		Pipeline p = Pipeline.create(options);

		// Build the table schema for the output table.
		List<TableFieldSchema> fields = new ArrayList<>();
		fields.add(new TableFieldSchema().setName("pageid").setType("INTEGER"));
		fields.add(new TableFieldSchema().setName("rqsts_per_page").setType("INTEGER"));
		TableSchema schema = new TableSchema().setFields(fields);

		p.apply(BigQueryIO.Read.from(options.getInput()))
		.apply(new CountRqstsPerPage())
		.apply(BigQueryIO.Write
				.to(options.getOutput())
				.withSchema(schema)
				.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
				.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));

		p.run();
	}
}
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
 * An example that reads the public samples of http archive data from BigQuery, analyzes the popularity
 * of different CDN providers, and writes the results to BigQuery.
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
public class BigQueryPopularCDN {
	// Default data set: httparchive:runs.2016_04_15_requests
	private static final String HTTP_ARCHIVE_TABLE =
			"httparchive:runs.2016_04_15_requests";
			// "gleaming-entry-130201:hqiu_httparchive_result.2016_04_15_requests_sample";

	private static final String CDN_PROVIDER_TABLE =
			"gleaming-entry-130201:hqiu_httparchive_result.CDNProviderPopularity";

	/**
	 * Examines each row in the input table. If a CDN provider is recorded
	 * in that sample, the name of the CDN provider is the output.
	 */
	static class ExtractCDNProviderFn extends DoFn<TableRow, String> {
		@Override
		public void processElement(ProcessContext c){
			TableRow row = c.element();
			if ((String) row.get("_cdn_provider") != null && 
					! ((String) row.get("_cdn_provider")).isEmpty()) {
				c.output((String) row.get("_cdn_provider"));
			}
		}
	}

	/**
	 * Prepares the data for writing to BigQuery by building a TableRow object containing the
	 * CDN Providers and the occurrences of the providers.
	 */
	static class FormatCountsFn extends DoFn<KV<String, Long>, TableRow> {
		@Override
		public void processElement(ProcessContext c) {
			TableRow row = new TableRow()
					.set("provider", c.element().getKey())
					.set("number", c.element().getValue());
			c.output(row);
		}
	}

	/**
	 * Takes rows from a table and generates a table of counts.
	 *
	 * The output contains the total number of CDN provides found in each requests in
	 * the following schema:
	 * <ul>
	 *   <li>provider: string</li>
	 *   <li>number: integer</li>
	 * </ul>
	 */
	static class CountCDNProviders
	extends PTransform<PCollection<TableRow>, PCollection<TableRow>> {
		@Override
		public PCollection<TableRow> apply(PCollection<TableRow> rows) {

			// row... => CDN provider...
			PCollection<String> CDNProviders = rows.apply(
					ParDo.of(new ExtractCDNProviderFn()));

			// CDN provider... => <CDN provider,count>...
			PCollection<KV<String, Long>> CDNProviderCounts =
					CDNProviders.apply(Count.<String>perElement());

			// <CDN provider,count>... => row...
			PCollection<TableRow> results = CDNProviderCounts.apply(
					ParDo.of(new FormatCountsFn()));

			return results;
		}
	}

	/**
	 * Options supported by {@link BigQueryPopularCDN}.
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
		@Default.String(CDN_PROVIDER_TABLE)
		@Validation.Required
		String getOutput();
		void setOutput(String value);
	}

	public static void main(String[] args) {
		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

		Pipeline p = Pipeline.create(options);

		// Build the table schema for the output table.
		List<TableFieldSchema> fields = new ArrayList<>();
		fields.add(new TableFieldSchema().setName("provider").setType("STRING"));
		fields.add(new TableFieldSchema().setName("number").setType("INTEGER"));
		TableSchema schema = new TableSchema().setFields(fields);

		p.apply(BigQueryIO.Read.from(options.getInput()))
		.apply(new CountCDNProviders())
		.apply(BigQueryIO.Write
				.to(options.getOutput())
				.withSchema(schema)
				.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
				.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));

		p.run();
	}
}
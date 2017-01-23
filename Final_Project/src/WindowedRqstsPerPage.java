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
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;

import my.java.packageid.WordCount.ExtractWordsFn;
import my.java.packageid.common.DataflowExampleUtils;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.PipelineResult;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.PubsubIO;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.options.Validation;
import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.DoFn.ProcessContext;
import com.google.cloud.dataflow.sdk.transforms.windowing.FixedWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;

import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class WindowedRqstsPerPage {
	// Default data set: httparchive:runs.2016_04_15_requests
	private static final String HTTP_ARCHIVE_TABLE =
			"httparchive:runs.2016_04_15_requests";
			// "gleaming-entry-130201:hqiu_httparchive_result.2016_04_15_requests_sample";
	
	private static final String WINDOWED_RQST_PER_PAGE_TABLE =
			"gleaming-entry-130201:hqiu_httparchive_result.WindowedRqstsPerPage";
	
	private static final Logger LOG = LoggerFactory.getLogger(WindowedRqstsPerPage.class);
	static final int WINDOW_SIZE = 20;  // Default window duration in minutes
	
	static class AddTimestampFn extends DoFn<TableRow, String> {
		private static final long RAND_RANGE = 3600000; // 1 hours in ms

		@Override
		public void processElement(ProcessContext c) {
			TableRow row = c.element();
			String pageid = (String) row.get("pageid");
			
			// Generate a timestamp that falls somewhere in the past hour.
			long randomTimestamp = System.currentTimeMillis()
					- (int) (Math.random() * RAND_RANGE);
			/**
			 * Concept #2: Set the data element with that timestamp.
			 */
			c.outputWithTimestamp(pageid, new Instant(randomTimestamp));
		}
	}
	
	static class ExtractPageidFn extends DoFn<String, String> {
		@Override
		public void processElement(ProcessContext c){
			String pageid = c.element();
			c.output(pageid);
		}
	}
	
	static class CountRqstsPerPage extends PTransform<PCollection<String>,
	PCollection<KV<String, Long>>> {
		@Override
		public PCollection<KV<String, Long>> apply(PCollection<String> lines) {

			// Convert lines of text into individual page id.
			PCollection<String> pageid = lines.apply(
					ParDo.of(new ExtractPageidFn()));

			// Count the number of times each page id occurs.
			PCollection<KV<String, Long>> rqstCounts =
					pageid.apply(Count.<String>perElement());

			return rqstCounts;
		}
	}

	/** A DoFn that converts a page id and Count into a BigQuery table row. */
	static class FormatAsTableRowFn extends DoFn<KV<String, Long>, TableRow> {
		@Override
		public void processElement(ProcessContext c) {
			TableRow row = new TableRow()
					.set("pageid", c.element().getKey())
					.set("count", c.element().getValue())
					// include a field for the window timestamp
					.set("window_timestamp", c.timestamp().toString());
			c.output(row);
		}
	}

	/**
	 * Helper method that defines the BigQuery schema used for the output.
	 */
	private static TableSchema getSchema() {
		List<TableFieldSchema> fields = new ArrayList<>();
		fields.add(new TableFieldSchema().setName("pageid").setType("INTEGER"));
		fields.add(new TableFieldSchema().setName("count").setType("INTEGER"));
		fields.add(new TableFieldSchema().setName("window_timestamp").setType("TIMESTAMP"));
		TableSchema schema = new TableSchema().setFields(fields);
		return schema;
	}

	/**
	 * Concept #6: We'll stream the results to a BigQuery table. The BigQuery output source is one
	 * that supports both bounded and unbounded data. This is a helper method that creates a
	 * TableReference from input options, to tell the pipeline where to write its BigQuery results.
	 */
	private static TableReference getTableReference(Options options) {
		TableReference tableRef = new TableReference();
		tableRef.setProjectId(options.getProject());
		tableRef.setDatasetId(options.getBigQueryDataset());
		tableRef.setTableId(options.getBigQueryTable());
		return tableRef;
	}

	/**
	 * Options supported by {@link WindowedRqstsPerPage}.
	 *
	 * <p>Inherits standard example configuration options, which allow specification of the BigQuery
	 * table and the PubSub topic, as well as the {@link WordCount.WordCountOptions} support for
	 * specification of the input file.
	 */
	public static interface Options
	extends PipelineOptions, DataflowExampleUtils.DataflowExampleUtilsOptions {
		@Description("Fixed window duration, in minutes")
		@Default.Integer(WINDOW_SIZE)
		Integer getWindowSize();
		void setWindowSize(Integer value);

		@Description("Table to read from, specified as "
				+ "<project_id>:<dataset_id>.<table_id>")
		@Default.String(HTTP_ARCHIVE_TABLE)
		String getInput();
		void setInput(String value);
		
		@Description("BigQuery table to write to, specified as "
				+ "<project_id>:<dataset_id>.<table_id>. The dataset must already exist.")
		@Default.String(WINDOWED_RQST_PER_PAGE_TABLE)
		@Validation.Required
		String getOutput();
		void setOutput(String value);
		
		@Description("Whether to run the pipeline with unbounded input")
		// @Default.Boolean(value = true)
		boolean isUnbounded();
		void setUnbounded(boolean value);
	}

	public static void main(String[] args) throws IOException {
		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
		options.setBigQuerySchema(getSchema());
		// DataflowExampleUtils creates the necessary input sources to simplify execution of this
		// Pipeline.
		DataflowExampleUtils exampleDataflowUtils = new DataflowExampleUtils(options,
				options.isUnbounded());

		Pipeline pipeline = Pipeline.create(options);

		/**
		 * Concept #1: the Dataflow SDK lets us run the same pipeline with either a bounded or
		 * unbounded input source.
		 */
		PCollection<String> input;
		if (options.isUnbounded()) {
			LOG.info("Reading from PubSub.");
			/**
			 * Concept #3: Read from the PubSub topic. A topic will be created if it wasn't
			 * specified as an argument. The data elements' timestamps will come from the pubsub
			 * injection.
			 */
			input = pipeline
					.apply(PubsubIO.Read.topic(options.getPubsubTopic()));
		} else {
			/** Else, this is a bounded pipeline. Read from the GCS file. */
			input = pipeline
					.apply(BigQueryIO.Read.from(options.getInput()))
					// Concept #2: Add an element timestamp, using an artificial time just to show windowing.
					// See AddTimestampFn for more detail on this.
					.apply(ParDo.of(new AddTimestampFn()));
		}

		/**
		 * Concept #4: Window into fixed windows. The fixed window size for this example defaults to 10
		 * minute (you can change this with a command-line option). See the documentation for more
		 * information on how fixed windows work, and for information on the other types of windowing
		 * available (e.g., sliding windows).
		 */
		PCollection<String> windowedRqsts = input
				.apply(Window.<String>into(
						FixedWindows.of(Duration.standardMinutes(options.getWindowSize()))));

		/**
		 * Concept #5: Re-use our existing CountWords transform that does not have knowledge of
		 * windows over a PCollection containing windowed values.
		 */
		PCollection<KV<String, Long>> rqstsCounts = windowedRqsts.apply(new CountRqstsPerPage());

		/**
		 * Concept #6: Format the results for a BigQuery table, then write to BigQuery.
		 * The BigQuery output source supports both bounded and unbounded data.
		 */
		rqstsCounts.apply(ParDo.of(new FormatAsTableRowFn()))
		.apply(BigQueryIO.Write.to(getTableReference(options)).withSchema(getSchema()));

		PipelineResult result = pipeline.run();

		/**
		 * To mock unbounded input from PubSub, we'll now start an auxiliary 'injector' pipeline that
		 * runs for a limited time, and publishes to the input PubSub topic.
		 *
		 * With an unbounded input source, you will need to explicitly shut down this pipeline when you
		 * are done with it, so that you do not continue to be charged for the instances. You can do
		 * this via a ctrl-C from the command line, or from the developer's console UI for Dataflow
		 * pipelines. The PubSub topic will also be deleted at this time.
		 */
		exampleDataflowUtils.mockUnboundedSource(options.getInput(), result);
	}
}

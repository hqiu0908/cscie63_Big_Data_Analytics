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
import com.google.cloud.dataflow.sdk.transforms.View;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionView;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

/**
 * This is an example that demonstrates several approaches to filtering, and use of the Mean
 * transform. It shows how to dynamically set parameters by defining and using new pipeline options,
 * and how to use a value derived by the pipeline.
 *
 * <p>Concepts: The Mean transform; Options configuration; using pipeline-derived data as a side
 * input; approaches to filtering, selection, and projection.
 *
 * <p>The example reads public samples of http archive data from BigQuery. It performs a
 * projection on the data, finds the global mean of the image object size, filters on readings
 * for a single given format, and then outputs only data (for that format) that has a size
 * larger than the derived global mean.
 *
 * <p>Note: Before running this example, you must create a BigQuery dataset to contain your output
 * table.
 *
 * <p>To execute this pipeline locally, specify general pipeline configuration:
 * <pre>{@code
 *   --project=YOUR_PROJECT_ID
 * }
 * </pre>
 * and the BigQuery table for the output:
 * <pre>{@code
 *   --output=YOUR_PROJECT_ID:DATASET_ID.TABLE_ID
 *   [--formatFilter=<format_number>]
 * }
 * </pre>
 * where optional parameter {@code --formatFilter} is set to a string:
 *  gif, png, jpg, svg...
 *
 * <p>To execute this pipeline using the Dataflow service, specify pipeline configuration:
 * <pre>{@code
 *   --project=YOUR_PROJECT_ID
 *   --stagingLocation=gs://YOUR_STAGING_DIRECTORY
 *   --runner=BlockingDataflowPipelineRunner
 * }
 * <p>The BigQuery input table defaults to {@code httparchive:runs.2016_04_15_requests}
 * and can be overridden with {@code --input}.
 */
public class FilterImgObjSize {
	// Default data set: httparchive:runs.2016_04_15_requests
	private static final String HTTP_ARCHIVE_TABLE =
			"httparchive:runs.2016_04_15_requests";
	// "gleaming-entry-130201:hqiu_httparchive_result.2016_04_15_requests_sample";

	private static final String IMG_OBJ_SIZE_TABLE =
			"gleaming-entry-130201:hqiu_httparchive_result.FilterImgObjSize";

	static final Logger LOG = Logger.getLogger(FilterImgObjSize.class.getName());
	static final String FORMAT_TO_FILTER = "svg";

	/**
	 * Examines each row in the input table. Outputs only the subset of the cells this example
	 * is interested in-- the type, format, and resp_content_length -- as a bigquery table row.
	 */
	static class ProjectionFn extends DoFn<TableRow, TableRow> {
		@Override
		public void processElement(ProcessContext c){
			TableRow row = c.element();
			// Grab type, format, and resp_content_length from the row
			String type = (String) row.get("type");
			String format = (String) row.get("format");
			String length = (String) row.get("resp_content_length");

			// Escape invalid content length
			if (length.contains(";") || length.contains(" ") || length.contains(",")
					|| (length.length() >= 10 && length.charAt(0) > '1')) {
				return;
			}

			// Prepares the data for writing to BigQuery by building a TableRow object
			if (type != null && type.equals("image")
					&& format != null && ! format.isEmpty()
					&& length != null && ! length.isEmpty()) {
				TableRow outRow = new TableRow()
						.set("type", type)
						.set("format", format)
						.set("length", length);
				c.output(outRow);
			}
		}
	}

	/**
	 * Implements 'filter' functionality.
	 *
	 * <p>Examines each row in the input table. Outputs only rows from the format
	 * formatFilter, which is passed in as a parameter during construction of this DoFn.
	 */
	static class FilterSingleFormatDataFn extends DoFn<TableRow, TableRow> {
		String formatFilter;

		public FilterSingleFormatDataFn(String formatFilter) {
			this.formatFilter = formatFilter;
		}

		@Override
		public void processElement(ProcessContext c){
			TableRow row = c.element();
			String format = (String) row.get("format");
			if (format.equals(this.formatFilter)) {
				c.output(row);
			}
		}
	}

	/**
	 * Examines each row (weather reading) in the input table. Output the temperature
	 * reading for that row ('mean_temp').
	 */
	static class ExtractLengthFn extends DoFn<TableRow, Double> {
		@Override
		public void processElement(ProcessContext c){
			TableRow row = c.element();
			Double contentLength = Double.parseDouble((String) row.get("length"));
			c.output(contentLength);
		}
	}

	/*
	 * Finds the global mean of the mean_length for each record, and outputs
	 * only data that has a mean content length larger than this global mean.
	 **/
	static class BelowGlobalMean
	extends PTransform<PCollection<TableRow>, PCollection<TableRow>> {
		String formatFilter;

		public BelowGlobalMean(String formatFilter) {
			this.formatFilter = formatFilter;
		}

		@Override
		public PCollection<TableRow> apply(PCollection<TableRow> rows) {

			// Extract the resp_content_length from each row.
			PCollection<Double> length = rows.apply(
					ParDo.of(new ExtractLengthFn()));

			// Find the global mean, of all the resp_content_length readings,
			// and prepare this singleton PCollectionView for use as a side input.
			final PCollectionView<Double> globalMeanLength =
					length.apply(Mean.<Double>globally())
					.apply(View.<Double>asSingleton());

			// Rows filtered to remove all but a single format
			PCollection<TableRow> formatFilteredRows = rows
					.apply(ParDo.of(new FilterSingleFormatDataFn(formatFilter)));

			// Then, use the global mean as a side input, to further filter the data.
			// By using a side input to pass in the filtering criteria, we can use a value
			// that is computed earlier in pipeline execution.
			// We'll only output readings with temperatures below this mean.
			PCollection<TableRow> filteredRows = formatFilteredRows
					.apply(ParDo
							.named("ParseAndFilter")
							.withSideInputs(globalMeanLength)
							.of(new DoFn<TableRow, TableRow>() {
								@Override
								public void processElement(ProcessContext c) {
									Double contentLength = Double.parseDouble(c.element().get("length").toString());
									Double gLenth = c.sideInput(globalMeanLength);
									if (contentLength > gLenth) {
										c.output(c.element());
									}
								}
							}));

			return filteredRows;
		}
	}


	/**
	 * Options supported by {@link FilterImgObjSize}.
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
				+ "<project_id>:<dataset_id>.<table_id>. "
				+ "The dataset_id must already exist")
		@Default.String(IMG_OBJ_SIZE_TABLE)
		@Validation.Required
		String getOutput();
		void setOutput(String value);

		@Description("Numeric value of format to filter on")
		@Default.String(FORMAT_TO_FILTER)
		String getFormatFilter();
		void setFormatFilter(String value);
	}

	/**
	 * Helper method to build the table schema for the output table.
	 */
	private static TableSchema buildOutputSchemaProjection() {
		List<TableFieldSchema> fields = new ArrayList<>();
		fields.add(new TableFieldSchema().setName("type").setType("STRING"));
		fields.add(new TableFieldSchema().setName("format").setType("STRING"));
		fields.add(new TableFieldSchema().setName("length").setType("FLOAT"));
		TableSchema schema = new TableSchema().setFields(fields);
		return schema;
	}

	public static void main(String[] args)
			throws Exception {

		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
		Pipeline p = Pipeline.create(options);

		TableSchema schema = buildOutputSchemaProjection();

		p.apply(BigQueryIO.Read.from(options.getInput()))
		.apply(ParDo.of(new ProjectionFn()))
		.apply(new BelowGlobalMean(options.getFormatFilter()))
		.apply(BigQueryIO.Write
				.to(options.getOutput())
				.withSchema(schema)
				.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
				.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));

		p.run();
	}
}
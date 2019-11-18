/*
 * ------------------------------------------------------------------------
 *
 *  Copyright by KNIME AG, Zurich, Switzerland
 *  Website: http://www.knime.com; Email: contact@knime.com
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License, Version 3, as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses>.
 *
 *  Additional permission under GNU GPL version 3 section 7:
 *
 *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
 *  Hence, KNIME and ECLIPSE are both independent programs and are not
 *  derived from each other. Should, however, the interpretation of the
 *  GNU GPL Version 3 ("License") under any applicable laws result in
 *  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
 *  you the additional permission to use and propagate KNIME together with
 *  ECLIPSE with only the license terms in place for ECLIPSE applying to
 *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
 *  license terms of ECLIPSE themselves allow for the respective use and
 *  propagation of ECLIPSE together with KNIME.
 *
 *  Additional permission relating to nodes for KNIME that extend the Node
 *  Extension (and in particular that are based on subclasses of NodeModel,
 *  NodeDialog, and NodeView) and that only interoperate with KNIME through
 *  standard APIs ("Nodes"):
 *  Nodes are deemed to be separate and independent programs and to not be
 *  covered works.  Notwithstanding anything to the contrary in the
 *  License, the License does not apply to Nodes, you are not required to
 *  license Nodes under the License, and you are granted a license to
 *  prepare and propagate Nodes, in each case even if such Nodes are
 *  propagated with or for interoperation with KNIME.  The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for interoperation with KNIME.
 * ---------------------------------------------------------------------
 *
 * History
 *   Oct 28, 2019 (Simon Schmid, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.cloud.aws.mlservices.nodes.personalize.upload;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Optional;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.SchemaBuilder.FieldAssembler;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.base.filehandling.remote.files.Connection;
import org.knime.base.filehandling.remote.files.ConnectionMonitor;
import org.knime.base.filehandling.remote.files.RemoteFile;
import org.knime.base.filehandling.remote.files.RemoteFileFactory;
import org.knime.base.node.io.csvwriter.CSVWriter;
import org.knime.base.node.io.csvwriter.FileWriterSettings;
import org.knime.cloud.aws.mlservices.personalize.AmazonPersonalizeConnection;
import org.knime.cloud.aws.mlservices.utils.personalize.AmazonPersonalizeUtils;
import org.knime.cloud.aws.mlservices.utils.personalize.AmazonPersonalizeUtils.Status;
import org.knime.cloud.aws.util.AmazonConnectionInformationPortObject;
import org.knime.cloud.core.util.port.CloudConnectionInformation;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.IntValue;
import org.knime.core.data.LongValue;
import org.knime.core.data.StringValue;
import org.knime.core.data.container.ColumnRearranger;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.util.FileUtil;

import com.amazonaws.services.personalize.AmazonPersonalize;
import com.amazonaws.services.personalize.model.CreateDatasetGroupRequest;
import com.amazonaws.services.personalize.model.CreateDatasetGroupResult;
import com.amazonaws.services.personalize.model.CreateDatasetImportJobRequest;
import com.amazonaws.services.personalize.model.CreateDatasetRequest;
import com.amazonaws.services.personalize.model.CreateSchemaRequest;
import com.amazonaws.services.personalize.model.DataSource;
import com.amazonaws.services.personalize.model.DatasetGroupSummary;
import com.amazonaws.services.personalize.model.DatasetSchemaSummary;
import com.amazonaws.services.personalize.model.DatasetSummary;
import com.amazonaws.services.personalize.model.DeleteDatasetGroupRequest;
import com.amazonaws.services.personalize.model.DeleteDatasetRequest;
import com.amazonaws.services.personalize.model.DescribeDatasetGroupRequest;
import com.amazonaws.services.personalize.model.DescribeDatasetGroupResult;
import com.amazonaws.services.personalize.model.DescribeDatasetImportJobRequest;
import com.amazonaws.services.personalize.model.DescribeDatasetImportJobResult;
import com.amazonaws.services.personalize.model.ListDatasetGroupsRequest;
import com.amazonaws.services.personalize.model.ListDatasetGroupsResult;
import com.amazonaws.services.personalize.model.ListDatasetsRequest;
import com.amazonaws.services.personalize.model.ListDatasetsResult;
import com.amazonaws.services.personalize.model.ListSchemasRequest;
import com.amazonaws.util.StringUtils;

/**
 * Abstract node model for Amazon Personalize data upload nodes.
 *
 * @author Simon Schmid, KNIME GmbH, Konstanz, Germany
 * @param <S> the object used to transfer settings from dialog to model
 */
public abstract class AbstractAmazonPersonalizeDataUploadNodeModel<S extends AbstractAmazonPersonalizeDataUploadNodeSettings>
    extends NodeModel {

    private static final String SCHEMA_NAMESPACE = "com.amazonaws.personalize.schema";

    /** */
    protected static final String PREFIX_METADATA_FIELD = "METADATA_";

    private final String m_datasetType = getDatasetType();

    /** The settings */
    protected S m_settings;

    /**
     * @return the settings
     */
    protected abstract S getSettings();

    /** */
    protected AbstractAmazonPersonalizeDataUploadNodeModel() {
        super(new PortType[]{AmazonConnectionInformationPortObject.TYPE, BufferedDataTable.TYPE}, null);
    }

    /**
     * @return the type of the dataset that should be uploaded
     */
    protected abstract String getDatasetType();

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        if (m_settings == null) {
            throw new InvalidSettingsException("The node must be configured.");
        }
        // no output
        return null;
    }

    /**
     * @param namespace the namespace
     * @return the field assembler created with the proper properties
     */
    protected abstract FieldAssembler<Schema> createFieldAssembler(final String namespace);

    /**
     * @return the prefix of the schema name
     */
    protected String getSchemaNamePrefix() {
        return m_settings.getPrefixSchemaName();
    }

    private String createSchema(final AmazonPersonalize personalizeClient, final DataTableSpec spec) {
        final StringBuilder schemaNameBuilder = new StringBuilder(getSchemaNamePrefix());
        FieldAssembler<Schema> fieldAssembler = createFieldAssembler(SCHEMA_NAMESPACE);
        for (final String colName : spec.getColumnNames()) {
            if (!colName.startsWith(PREFIX_METADATA_FIELD)) {
                continue;
            }
            final DataColumnSpec colSpec = spec.getColumnSpec(colName);
            final boolean isCategorical;
            final Type type;
            if (colSpec.getType().isCompatible(StringValue.class)) {
                isCategorical = true;
                type = Type.STRING;
            } else if (colSpec.getType().isCompatible(IntValue.class)) {
                isCategorical = false;
                type = Type.INT;
            } else if (colSpec.getType().isCompatible(LongValue.class)) {
                isCategorical = false;
                type = Type.LONG;
            } else {
                isCategorical = false;
                type = Type.DOUBLE;
            }
            schemaNameBuilder.append("-" + type);
            // 'categorical' must be set for metadata
            fieldAssembler =
                fieldAssembler.name(colName).prop("categorical", isCategorical).type(Schema.create(type)).noDefault();
        }
        final String schemaName = schemaNameBuilder.toString();

        // check if the same schema has been created before
        final List<DatasetSchemaSummary> existingSchemas =
            personalizeClient.listSchemas(new ListSchemasRequest()).getSchemas();
        final Optional<DatasetSchemaSummary> schemaSummary =
            existingSchemas.stream().filter(e -> e.getName().equals(schemaName)).findAny();
        // if so, use this one again
        if (schemaSummary.isPresent()) {
            return schemaSummary.get().getSchemaArn();
        }
        // otherwise create new one
        final Schema schema = fieldAssembler.endRecord();
        final CreateSchemaRequest createSchemaRequest =
            new CreateSchemaRequest().withName(schemaName).withSchema(schema.toString());
        return personalizeClient.createSchema(createSchemaRequest).getSchemaArn();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] execute(final PortObject[] inObjects, final ExecutionContext exec) throws Exception {

        // === Write table out as CSV ===  (TODO we may be able to write it directly to S3)
        // Filter included columns
        final BufferedDataTable filterTable = filterTable((BufferedDataTable)inObjects[1], exec);
        // Rename columns to fit the later created schema
        final BufferedDataTable adaptedTable = renameColumns(filterTable, exec);
        // Write the table as CSV to disc
        final URI sourceURI = writeCSV(adaptedTable, exec.createSubExecutionContext(0.1));

        // === Upload CSV to S3 ===
        final CloudConnectionInformation cxnInfo =
            ((AmazonConnectionInformationPortObject)inObjects[0]).getConnectionInformation();
        final String uniqueFilePath = m_settings.getTarget() + "KNIME-tmp-" + StringUtils.lowerCase(getDatasetType())
            + "-file-" + System.currentTimeMillis() + ".csv";
        final RemoteFile<Connection> target =
            writeToS3(exec.createSubExecutionContext(0.1), sourceURI, cxnInfo, uniqueFilePath);

        // === Import data from S3 to Amazon Personalize service ===
        try (final AmazonPersonalizeConnection personalizeConnection = new AmazonPersonalizeConnection(cxnInfo)) {
            final AmazonPersonalize personalizeClient = personalizeConnection.getClient();

            // Create the dataset group ARN or use existing one
            final String datasetGroupArn = createDatasetGroup(personalizeClient, exec.createSubExecutionContext(0.2));

            // Check if the respective dataset already exists and either delete it or abort
            checkAlreadyExistingDataset(personalizeClient, datasetGroupArn, exec.createSubExecutionContext(0.1));

            // Create the data set (container)
            exec.setMessage("Importing dataset from S3");
            final String schemaArn = createSchema(personalizeClient, adaptedTable.getDataTableSpec());
            final String datasetArn = personalizeClient
                .createDataset(new CreateDatasetRequest().withDatasetGroupArn(datasetGroupArn)
                    .withDatasetType(m_datasetType).withName(m_settings.getDatasetName()).withSchemaArn(schemaArn))
                .getDatasetArn();
            try {
                // Import the dataset from S3
                importDataFromS3(personalizeClient, "s3:/" + uniqueFilePath, datasetArn, exec);
            } catch (RuntimeException | InterruptedException e1) {
                try {
                    deleteDataset(personalizeClient, datasetGroupArn, datasetArn);
                } catch (InterruptedException e) {
                    // happens if user cancels node execution during deletion of dataset
                    // do nothing, deletion will be further processed by amazon
                }
                throw e1;
            }
        } catch (RuntimeException e) {
            // TODO cancel import job, currently not supported but hopefully in future versions
            throw e;
        } finally {
            // Remove temporary created S3 file
            target.delete();
        }

        return null;
    }

    private void importDataFromS3(final AmazonPersonalize personalizeClient, final String s3FilePath,
        final String datasetArn, final ExecutionContext exec) throws InterruptedException {

        // Start the job that imports the dataset from S3
        final DataSource dataSource = new DataSource().withDataLocation(s3FilePath);
        final String jobName = m_settings.getPrefixImportJobName() + System.currentTimeMillis();
        final String datasetImportJobArn = personalizeClient
            .createDatasetImportJob(new CreateDatasetImportJobRequest().withDatasetArn(datasetArn)
                .withRoleArn(m_settings.getIamServiceRoleArn()).withDataSource(dataSource).withJobName(jobName))
            .getDatasetImportJobArn();

        // Wait until status of dataset is ACTIVE
        final DescribeDatasetImportJobRequest describeDatasetImportJobRequest =
            new DescribeDatasetImportJobRequest().withDatasetImportJobArn(datasetImportJobArn);
        AmazonPersonalizeUtils.waitUntilActive(() -> {
            final DescribeDatasetImportJobResult datasetImportJobDescription =
                personalizeClient.describeDatasetImportJob(describeDatasetImportJobRequest);
            final String status = datasetImportJobDescription.getDatasetImportJob().getStatus();
            exec.setMessage("Importing dataset from S3 (Status: " + status + ")");
            if (status.equals(Status.CREATED_FAILED.getStatus())) {
                throw new IllegalStateException("No dataset has been created. Reason: "
                    + datasetImportJobDescription.getDatasetImportJob().getFailureReason());
            }
            return status.equals(Status.ACTIVE.getStatus());
        }, 2000);
    }

    private void checkAlreadyExistingDataset(final AmazonPersonalize personalizeClient, final String datasetGroupArn,
        final ExecutionContext exec) throws InterruptedException {
        exec.setMessage("Checking already existing datasets");
        final ListDatasetsResult listDatasets =
            personalizeClient.listDatasets(new ListDatasetsRequest().withDatasetGroupArn(datasetGroupArn));
        final Optional<DatasetSummary> dataset =
            listDatasets.getDatasets().stream().filter(e -> e.getDatasetType().equals(m_datasetType)).findFirst();
        if (dataset.isPresent()) {
            if (m_settings.getOverwriteDatasetPolicy().equals(OverwritePolicy.ABORT.toString())) {
                // Abort if dataset already exists
                throw new IllegalStateException("A dataset of type '" + getDatasetType()
                    + "' already exists. Either choose a different dataset group or select to overwrite the existing "
                    + "dataset.");
            } else {
                // Delete the existing dataset
                exec.setMessage("Deleting existing dataset");
                deleteDataset(personalizeClient, datasetGroupArn, dataset.get().getDatasetArn());
            }
        }
        exec.setProgress(1);
    }

    private void deleteDataset(final AmazonPersonalize personalizeClient, final String datasetGroupArn,
        final String datasetARN) throws InterruptedException {
        personalizeClient.deleteDataset(new DeleteDatasetRequest().withDatasetArn(datasetARN));

        final ListDatasetsRequest listDatasetsRequest = new ListDatasetsRequest().withDatasetGroupArn(datasetGroupArn);
        AmazonPersonalizeUtils.waitUntilActive(() -> {
            final List<DatasetSummary> datasets = personalizeClient.listDatasets(listDatasetsRequest).getDatasets();
            return !datasets.stream().anyMatch(e -> e.getDatasetType().equals(m_datasetType));
        }, 500);
    }

    // Creates a new dataset group if not already existing
    private String createDatasetGroup(final AmazonPersonalize personalizeClient, final ExecutionContext exec)
        throws InterruptedException {
        exec.setMessage("Creating dataset group");
        final ListDatasetGroupsRequest listDatasetGroupsRequest = new ListDatasetGroupsRequest();
        final ListDatasetGroupsResult listDatasetGroups = personalizeClient.listDatasetGroups(listDatasetGroupsRequest);
        final String datasetGroupName = m_settings.getSelectedDatasetGroup();
        final String datasetGroupArn;
        final boolean existing =
            listDatasetGroups.getDatasetGroups().stream().anyMatch(e -> e.getName().equals(datasetGroupName));
        if (!existing) {
            // Create new dataset group
            final CreateDatasetGroupResult createDatasetGroup =
                personalizeClient.createDatasetGroup(new CreateDatasetGroupRequest().withName(datasetGroupName));
            datasetGroupArn = createDatasetGroup.getDatasetGroupArn();
        } else {
            final Optional<DatasetGroupSummary> dataGroupSummary = listDatasetGroups.getDatasetGroups().stream()
                .filter(e -> e.getName().equals(datasetGroupName)).findFirst();
            if (!dataGroupSummary.isPresent()) {
                // should never happen
                throw new IllegalStateException("Dataset group with name '" + datasetGroupName + "' not present.");
            }
            datasetGroupArn = dataGroupSummary.get().getDatasetGroupArn();
        }

        // Wait until dataset group is created and ACTIVE (even if the group already existed, make sure it's ACTIVE)
        final DescribeDatasetGroupRequest describeDatasetGroupRequest = new DescribeDatasetGroupRequest();
        describeDatasetGroupRequest.setDatasetGroupArn(datasetGroupArn);
        AmazonPersonalizeUtils.waitUntilActive(() -> {
            final DescribeDatasetGroupResult datasetGroupDescription =
                personalizeClient.describeDatasetGroup(describeDatasetGroupRequest);
            final String status = datasetGroupDescription.getDatasetGroup().getStatus();
            exec.setMessage("Creating dataset group (Status: " + status + ")");
            if (status.equals(Status.CREATED_FAILED.getStatus())) {
                if (!existing) {
                    // Delete the dataset group that we tried to create
                    personalizeClient
                        .deleteDatasetGroup(new DeleteDatasetGroupRequest().withDatasetGroupArn(datasetGroupArn));
                    // Wait until the dataset group is deleted (should usually be very quick but you never know...)
                    try {
                        AmazonPersonalizeUtils.waitUntilActive(() -> {
                            return !personalizeClient.listDatasetGroups(listDatasetGroupsRequest).getDatasetGroups()
                                .stream().anyMatch(e -> e.getName().equals(datasetGroupName));
                        }, 50);
                    } catch (InterruptedException e1) {
                        // unlikely case
                        // do nothing, the deletion will be further processed by amazon
                    }
                    throw new IllegalStateException("Dataset group creation failed. Reason: "
                        + datasetGroupDescription.getDatasetGroup().getFailureReason());
                }
                throw new IllegalStateException(
                    "The selected dataset group is in an invalid state: " + Status.CREATED_FAILED.getStatus()
                        + ". Reason: " + datasetGroupDescription.getDatasetGroup().getFailureReason());
            }
            return status.equals(Status.ACTIVE.getStatus());
        }, 500);
        exec.setProgress(1);
        return datasetGroupArn;
    }

    // keep only included columns (required and metadata)
    private BufferedDataTable filterTable(final BufferedDataTable table, final ExecutionContext exec)
        throws CanceledExecutionException {
        final String[] includes = m_settings.getFilterConfig().applyTo(table.getDataTableSpec()).getIncludes();
        final ColumnRearranger columnRearranger = new ColumnRearranger(table.getDataTableSpec());
        columnRearranger.keepOnly(includes);
        final BufferedDataTable filteredTable = exec.createColumnRearrangeTable(table, columnRearranger, exec);
        return filteredTable;
    }

    /**
     * Returns a table that fulfills the naming requirements of Amazon Personalize for different dataset types.
     *
     * @param table input table
     * @param exec execution context
     * @return the input table with properly renamed columns
     * @throws CanceledExecutionException if the execution is canceled
     */
    protected abstract BufferedDataTable renameColumns(final BufferedDataTable table, final ExecutionContext exec)
        throws CanceledExecutionException;

    private static RemoteFile<Connection> writeToS3(final ExecutionContext exec, final URI sourceURI,
        final ConnectionInformation connectionInformation, final String filePath) throws URISyntaxException, Exception {
        final URI folderUri = new URI(connectionInformation.toURI().toString() + "/" + filePath);
        final ConnectionMonitor<Connection> monitor = new ConnectionMonitor<>();
        final RemoteFile<Connection> target =
            RemoteFileFactory.createRemoteFile(folderUri, connectionInformation, monitor);
        final RemoteFile<Connection> sourceFile = RemoteFileFactory.createRemoteFile(sourceURI, null, null);
        target.write(sourceFile, exec);
        return target;
    }

    @SuppressWarnings("resource")
    private static URI writeCSV(final BufferedDataTable table, final ExecutionContext exec)
        throws IOException, CanceledExecutionException {
        final File createTempDir = FileUtil.createTempFile("KNIME_amazon_personalize_", ".csv");
        final BufferedOutputStream tempOut = new BufferedOutputStream(new FileOutputStream(createTempDir));
        final FileWriterSettings settings = new FileWriterSettings();
        settings.setWriteColumnHeader(true);
        try (CSVWriter tableWriter = new CSVWriter(new OutputStreamWriter(tempOut, "UTF-8"), settings)) {
            tableWriter.write(table, exec);
        }
        return createTempDir.toURI();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadInternals(final File nodeInternDir, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {
        // nothing to do
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveInternals(final File nodeInternDir, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {
        // nothing to do
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        if (m_settings != null) {
            m_settings.saveSettings(settings);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        if (m_settings == null) {
            m_settings = getSettings();
        }
        m_settings.loadSettings(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void reset() {
        // nothing to do
    }

}

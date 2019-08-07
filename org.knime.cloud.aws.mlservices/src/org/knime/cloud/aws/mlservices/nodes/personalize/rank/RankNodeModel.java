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
 *   Apr 12, 2019 (jfalgout): created
 */
package org.knime.cloud.aws.mlservices.nodes.personalize.rank;

import java.io.File;
import java.io.IOException;

import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObjectSpec;
import org.knime.cloud.aws.util.AmazonConnectionInformationPortObject;
import org.knime.cloud.core.util.port.CloudConnectionInformation;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.StringValue;
import org.knime.core.data.collection.ListCell;
import org.knime.core.data.collection.ListDataValue;
import org.knime.core.data.def.StringCell;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.streamable.InputPortRole;
import org.knime.core.node.streamable.OutputPortRole;
import org.knime.core.node.streamable.PartitionInfo;
import org.knime.core.node.streamable.PortInput;
import org.knime.core.node.streamable.PortOutput;
import org.knime.core.node.streamable.RowInput;
import org.knime.core.node.streamable.RowOutput;
import org.knime.core.node.streamable.StreamableOperator;
import org.knime.ext.textprocessing.util.ColumnSelectionVerifier;

/**
 * Use the Amazon Personalize Runtime service to create recommendations for provided entities (users or items).
 *
 * @author KNIME AG, Zurich, Switzerland
 */
class RankNodeModel extends NodeModel {

    /** The logger instance */
    private static final NodeLogger LOGGER = NodeLogger.getLogger(RankNodeModel.class);

    /** Settings name for the input column name with the entity identifier. */
    private static final String CFG_USER_COL = "entity_column";

    /** Settings name for the input column name with the item identifiers. */
    private static final String CFG_ITEM_COL = "item_column";

    /** Settings name for the Personalize campaign ARN */
    private static final String CFG_CAMPAIGN_ARN = "campaign_arn";

    /**
     * Method to create a {@link SettingsModelString} storing the name of the input column containing the user id.
     */
    static final SettingsModelString getUserColModel() {
        return new SettingsModelString(CFG_USER_COL, null);
    }

    /**
     * Method to create a {@link SettingsModelString} storing the name of the input column containing the item id's.
     */
    static final SettingsModelString getItemColModel() {
        return new SettingsModelString(CFG_ITEM_COL, null);
    }

    /**
     * Method to create a {@link SettingsModelString} storing the ARN of the Personalize Campaign to use.
     */
    static final SettingsModelString getCampaignArnModel() {
        return new SettingsModelString(CFG_CAMPAIGN_ARN, null);
    }

    /** {@link SettingsModelString} storing the name of the column containing the user id. */
    private final SettingsModelString m_userCol= getUserColModel();

    /** {@link SettingsModelString} storing the name of the column containing the item id. */
    private final SettingsModelString m_itemCol= getItemColModel();

    /** {@link SettingsModelString} storing the ARN of the Campaign to use. */
    private final SettingsModelString m_campaignArn= getCampaignArnModel();

    /** Constructor for the node model */
    RankNodeModel() {
        // Inputs: connection info, data
        // Outputs: data
        super(new PortType[]{AmazonConnectionInformationPortObject.TYPE, BufferedDataTable.TYPE},
            new PortType[]{BufferedDataTable.TYPE});
    }

    @Override
    protected PortObject[] execute(final PortObject[] inObjects, final ExecutionContext exec) throws Exception {

        if (inObjects == null || inObjects.length != 2) {
            throw new InvalidSettingsException("Invalid input data. Expected two inputs.");
        }

        final CloudConnectionInformation cxnInfo =
            ((AmazonConnectionInformationPortObject)inObjects[0]).getConnectionInformation();
        LOGGER.info("Using region: " + cxnInfo.getHost());

        // Access the input data table
        final BufferedDataTable table = (BufferedDataTable)inObjects[1];

        // Create computation object for the recommendation operation.
        final RankOperation recommendOp =
                new RankOperation(
                    cxnInfo,
                    m_userCol.getStringValue(),
                    m_itemCol.getStringValue(),
                    m_campaignArn.getStringValue(),
                    createNewDataTableSpec(table.getDataTableSpec()));

        // Run the operation over the entire input.
        return new BufferedDataTable[]{recommendOp.compute(exec, table)};
    }

    @Override
    protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {

        if (inSpecs[0] != null) {
            final ConnectionInformationPortObjectSpec object =
                (ConnectionInformationPortObjectSpec)inSpecs[0];
            final ConnectionInformation cxnInfo = object.getConnectionInformation();
            // Check if the port object has connection information
            if (cxnInfo == null) {
                throw new InvalidSettingsException("No connection information available");
            }

//            if (!ConnectionUtils.regionSupported(cxnInfo.getHost(), AmazonPersonalize.ENDPOINT_PREFIX)) {
//                throw new InvalidSettingsException(
//                    "Unsupported region for the Amazon Personalize Runtime service: " + cxnInfo.getHost());
//            }
        } else {
            throw new InvalidSettingsException("No connection information available");
        }
        final DataTableSpec tblSpec = (DataTableSpec)inSpecs[1];
        checkDataTableSpec(tblSpec);

        return new DataTableSpec[]{createNewDataTableSpec(tblSpec)};
    }

    /**
     * Creates the new data table spec based on the input data table spec.
     *
     * @param inputSpec The input data table spec
     * @return Output data table spec
     */
    private static final DataTableSpec createNewDataTableSpec(final DataTableSpec inputSpec) {
        final DataColumnSpecCreator colSpec =
            new DataColumnSpecCreator("Rankings", ListCell.getCollectionType(StringCell.TYPE));
        return new DataTableSpec(inputSpec, new DataTableSpec(colSpec.createSpec()));
    }

    /**
     * Checks whether the input {@link DataTableSpec} is valid or not.
     *
     * @throws InvalidSettingsException Thrown if data table spec is invalid.
     */
    private void checkDataTableSpec(final DataTableSpec spec) throws InvalidSettingsException {
        final long numOfValidCols = spec.stream()//
            .filter(colSpec -> colSpec.getType().isCompatible(StringValue.class))//
            .count();
        if (numOfValidCols < 1) {
            throw new InvalidSettingsException("There has to be at least one column containing String data!");
        }

        ColumnSelectionVerifier.verifyColumn(m_userCol, spec, StringValue.class, null)
            .ifPresent(warningMsg -> setWarningMessage(warningMsg));

        ColumnSelectionVerifier.verifyColumn(m_itemCol, spec, ListDataValue.class, null)
            .ifPresent(warningMsg -> setWarningMessage(warningMsg));
    }

    @Override
    public StreamableOperator createStreamableOperator(final PartitionInfo partitionInfo,
        final PortObjectSpec[] inSpecs) throws InvalidSettingsException {

        final DataTableSpec dataSpec = (DataTableSpec)inSpecs[1];
        final ConnectionInformationPortObjectSpec cnxSpec = (ConnectionInformationPortObjectSpec)inSpecs[0];
        final CloudConnectionInformation cxnInfo = (CloudConnectionInformation)cnxSpec.getConnectionInformation();
        final RankOperation recommendOp =
                new RankOperation(
                    cxnInfo,
                    m_userCol.getStringValue(),
                    m_itemCol.getStringValue(),
                    m_campaignArn.getStringValue(),
                    createNewDataTableSpec(dataSpec));

        return new StreamableOperator() {

            @Override
            public void runFinal(final PortInput[] inputs, final PortOutput[] outputs, final ExecutionContext exec)
                throws Exception {
                RowInput input = (RowInput)inputs[1];
                RowOutput output = (RowOutput)outputs[0];
                recommendOp.compute(input, output, exec, 0L);
                input.close();
                output.close();
            }
        };
    }

    @Override
    public InputPortRole[] getInputPortRoles() {
        return new InputPortRole[]{InputPortRole.NONDISTRIBUTED_NONSTREAMABLE, InputPortRole.DISTRIBUTED_STREAMABLE};
    }

    @Override
    public OutputPortRole[] getOutputPortRoles() {
        return new OutputPortRole[]{OutputPortRole.DISTRIBUTED};
    }

    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        m_userCol.saveSettingsTo(settings);
        m_itemCol.saveSettingsTo(settings);
        m_campaignArn.saveSettingsTo(settings);

    }

    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_userCol.loadSettingsFrom(settings);
        m_itemCol.loadSettingsFrom(settings);
        m_campaignArn.loadSettingsFrom(settings);
    }

    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_userCol.validateSettings(settings);
        m_itemCol.validateSettings(settings);
        m_campaignArn.validateSettings(settings);
    }

    @Override
    protected void loadInternals(final File internDir, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {
        // Nothing to do here...
    }

    @Override
    protected void saveInternals(final File internDir, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {
        // Nothing to do here...
    }

    @Override
    protected void reset() {
        // Nothing to do here...
    }
}

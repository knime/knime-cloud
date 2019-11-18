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
package org.knime.cloud.aws.mlservices.nodes.personalize.predict;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObjectSpec;
import org.knime.cloud.aws.mlservices.personalize.AmazonPersonalizeRuntimeConnection;
import org.knime.cloud.aws.util.AmazonConnectionInformationPortObject;
import org.knime.cloud.core.util.port.CloudConnectionInformation;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.MissingCell;
import org.knime.core.data.collection.CollectionCellFactory;
import org.knime.core.data.collection.ListCell;
import org.knime.core.data.container.CellFactory;
import org.knime.core.data.container.ColumnRearranger;
import org.knime.core.data.container.SingleCellFactory;
import org.knime.core.data.def.StringCell;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.streamable.simple.SimpleStreamableFunctionNodeModel;
import org.knime.core.util.UniqueNameGenerator;

import com.amazonaws.services.personalizeruntime.AmazonPersonalizeRuntime;

/**
 * The abstract node model of the Amazon Personalize prediction nodes.
 *
 * @author KNIME AG, Zurich, Switzerland
 * @param <S> the settings class
 */
public abstract class AbstractAmazonPersonalizePredictNodeModel<S extends AmazonPersonalizePredictNodeSettings>
    extends SimpleStreamableFunctionNodeModel {

    /** The settings */
    protected S m_settings;

    private CloudConnectionInformation m_cxnInfo;

    boolean m_warningSet;

    /** Constructor for the node model */
    protected AbstractAmazonPersonalizePredictNodeModel() {
        super(new PortType[]{AmazonConnectionInformationPortObject.TYPE, BufferedDataTable.TYPE},
            new PortType[]{BufferedDataTable.TYPE}, 1, 0);
    }

    /**
     * @return the settings
     */
    protected abstract S getSettings();

    @Override
    protected PortObject[] execute(final PortObject[] inObjects, final ExecutionContext exec) throws Exception {
        return execute(new BufferedDataTable[]{(BufferedDataTable)inObjects[1]}, exec);
    }

    /**
     * @param personalizeConnection the connection
     * @param spec the table spec
     * @return the cell factory
     * @throws Exception
     */
    protected CellFactory getCellFactory(final AmazonPersonalizeRuntimeConnection personalizeConnection,
        final DataTableSpec spec) throws Exception {
        final UniqueNameGenerator uniqueNameGenerator = new UniqueNameGenerator(spec);
        if (m_settings == null) {
            m_settings = getSettings();
        }
        final int userIdColIdx = spec.findColumnIndex(m_settings.getUserIDCol());
        final int itemIdColIdx = spec.findColumnIndex(m_settings.getItemIDCol());
        final int itemsColIdx = spec.findColumnIndex(m_settings.getItemsCol());
        final HashMap<Integer, String> colMap = new HashMap<>();
        colMap.put(userIdColIdx, "user ID");
        colMap.put(itemIdColIdx, "item ID");
        colMap.put(itemsColIdx, "item list");

        m_warningSet = false;
        final AmazonPersonalizeRuntime personalizeClient = personalizeConnection.getClient();
        return new SingleCellFactory(true,
            uniqueNameGenerator.newColumn(getOutputColumnName(), ListCell.getCollectionType(StringCell.TYPE))) {

            final boolean m_failOnMissing = m_settings.getMissingValueHandling() == MissingValueHandling.FAIL;

            @Override
            public DataCell getCell(final DataRow row) {
                // Check for missing values
                MissingCell checkMissingValue = checkMissingValue(row, colMap, m_failOnMissing);
                if (checkMissingValue != null) {
                    return checkMissingValue;
                }

                // Get the prediction
                final ArrayList<DataCell> recommendations =
                    predict(personalizeClient, row, userIdColIdx, itemIdColIdx, itemsColIdx);
                return CollectionCellFactory.createListCell(recommendations);
            }
        };
    }

    /**
     * Predict the output.
     *
     * @param personalizeClient the client
     * @param row the row to predict
     * @param userIdColIdx the user column index
     * @param itemIdColIdx the item column index
     * @param itemsColIdx the item list column index
     * @return the prediction
     */
    protected abstract ArrayList<DataCell> predict(final AmazonPersonalizeRuntime personalizeClient, final DataRow row,
        final int userIdColIdx, final int itemIdColIdx, final int itemsColIdx);

    /**
     * @return the name of the output column
     */
    protected abstract String getOutputColumnName();

    private MissingCell checkMissingValue(final DataRow row, final Map<Integer, String> columns, final boolean fail) {
        for (final Integer colIdx : columns.keySet()) {
            final String colName = columns.get(colIdx);
            final String msg = "The " + colName + " in row '" + row.getKey() + "' is missing.";
            if (colIdx < 0) {
                continue;
            }
            final DataCell cell = row.getCell(colIdx);
            if (cell.isMissing()) {
                if (fail) {
                    throw new IllegalArgumentException(msg);
                } else {
                    if (!m_warningSet) {
                        setWarningMessage(msg + " A missing value will be the output.");
                        m_warningSet = true;
                    }
                    return new MissingCell(msg);
                }
            }
        }
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected ColumnRearranger createColumnRearranger(final DataTableSpec spec) throws InvalidSettingsException {
        ColumnRearranger columnRearranger = new ColumnRearranger(spec);

        final AmazonPersonalizeRuntimeConnection personalizeConnection =
            new AmazonPersonalizeRuntimeConnection(m_cxnInfo);
        try {
            columnRearranger.append(getCellFactory(personalizeConnection, spec));
        } catch (Exception e) {
            if (e instanceof RuntimeException) {
                throw (RuntimeException)e;
            } else {
                throw new IllegalStateException(e);
            }
        }
        return columnRearranger;
    }

    @Override
    protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        if (inSpecs[0] != null) {
            final ConnectionInformationPortObjectSpec object = (ConnectionInformationPortObjectSpec)inSpecs[0];
            m_cxnInfo = (CloudConnectionInformation)object.getConnectionInformation();
            // Check if the port object has connection information
            if (m_cxnInfo == null) {
                throw new InvalidSettingsException("No connection information available");
            }
        } else {
            throw new InvalidSettingsException("No connection information available");
        }
        if (m_settings == null) {
            throw new InvalidSettingsException("The node must be configured.");
        }
        return configure(new DataTableSpec[]{(DataTableSpec)inSpecs[1]});
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
        // nothing to do
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
}

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
 *   Mar 24, 2017 (oole): created
 */
package org.knime.cloud.aws.redshift.clustermanipulation.deleter;

import org.knime.cloud.aws.redshift.clustermanipulation.util.RedshiftGeneralSettings;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelString;

/**
 * The extended {@link RedshiftGeneralSettings} for the RedshiftClusterDeleterNodeModel.
 *
 * @author Ole Ostergaard, KNIME.com
 */
class RedshiftClusterDeleterNodeSettings extends RedshiftGeneralSettings {

    /**
     * Constructor.
     *
     * @param prefix The prefix for the connection
     */
    public RedshiftClusterDeleterNodeSettings(final String prefix) {
        super(prefix);
    }

    /** Holds the clusters name */
    protected final SettingsModelString m_clusterName = new SettingsModelString("clusterName", "");

    /** Whether a final cluster snapshot should be taken */
    protected final SettingsModelBoolean m_skipFinalClusterSnapshot =
        new SettingsModelBoolean("skipFinalSnapshot", true);

    /** Hold the final cluster snapshhot name */
    protected final SettingsModelString m_finalClusterSnapshotName =
        createFinalClusterSnapshotNameModel(m_skipFinalClusterSnapshot);

    private SettingsModelString createFinalClusterSnapshotNameModel(final SettingsModelBoolean skipSnapshot) {
        SettingsModelString model = new SettingsModelString("finalSnapshotName", "");
        skipSnapshot.addChangeListener(e -> model.setEnabled(!skipSnapshot.getBooleanValue()));
        model.setEnabled(!skipSnapshot.getBooleanValue());
        return model;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void saveSettingsTo(final NodeSettingsWO settings) {
        super.saveSettingsTo(settings);
        m_clusterName.saveSettingsTo(settings);
        m_skipFinalClusterSnapshot.saveSettingsTo(settings);
        m_finalClusterSnapshotName.saveSettingsTo(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void loadValidatedSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        super.loadValidatedSettings(settings);
        m_clusterName.loadSettingsFrom(settings);
        m_skipFinalClusterSnapshot.loadSettingsFrom(settings);
        m_finalClusterSnapshotName.loadSettingsFrom(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        super.validateSettings(settings);
        m_clusterName.validateSettings(settings);
        m_skipFinalClusterSnapshot.validateSettings(settings);
        m_finalClusterSnapshotName.validateSettings(settings);
    }

    /**
     * Returns the entered cluster name.
     *
     * @return The entered cluster name
     */
    public String getClusterName() {
        return m_clusterName.getStringValue();
    }

    /**
     * Returns whether the final snapshot should be skipped.
     *
     * @return Whether the final cluster snapshot should be skipped
     */
    public Boolean skipFinalClusterSnapshot() {
        return m_skipFinalClusterSnapshot.getBooleanValue();
    }

    /**
     * Returns the entered final snapshot name.
     *
     * @return The entered final snapshot name
     */
    public String getFinalClusterSnapshotName() {
        return m_finalClusterSnapshotName.getStringValue();
    }

    /**
     * Returns the model for the cluster name.
     *
     * @return The model for the cluster name
     */
    public SettingsModelString getClusterNameModel() {
        return m_clusterName;
    }

    /**
     * Returns the model for skipping of the final cluster snapshot.
     *
     * @return The model for the skipping of the final cluster snapshot
     */
    public SettingsModelBoolean getSkipFinalClusterSnapshotModel() {
        return m_skipFinalClusterSnapshot;
    }

    /**
     * Returns the model for the final cluster snapshot name.
     *
     * @return The model for the final cluster snapshot name
     */
    public SettingsModelString getFinalClusterSnapshotNameModel() {
        return m_finalClusterSnapshotName;
    }
}

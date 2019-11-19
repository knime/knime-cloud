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
 */
package org.knime.google.cloud.storage.node.connector;

import org.apache.commons.lang3.StringUtils;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.google.api.data.GoogleApiConnection;
import org.knime.google.cloud.storage.filehandler.GoogleCSRemoteFileHandler;
import org.knime.google.cloud.storage.util.GoogleCloudStorageConnectionInformation;

/**
 * {@link GoogleCSConnectionNodeModel} settings model.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class GoogleCSConnectionNodeSettings {

    private final SettingsModelString m_projectId = new SettingsModelString("projectId", "");

    GoogleCSConnectionNodeSettings() {
    }

    SettingsModelString getProjectIdModel() {
        return m_projectId;
    }

    /**
     * Saves the the settings of this instance to the given {@link NodeSettingsWO}.
     *
     * @param settings the NodeSettingsWO to write to.
     */
    public void saveSettingsTo(final NodeSettingsWO settings) {
        m_projectId.saveSettingsTo(settings);
    }

    /**
     * Validates the settings in the given {@link NodeSettingsRO}.
     *
     * @param settings the NodeSettingsRO to validate.
     * @throws InvalidSettingsException if the settings are invalid.
     */
    void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_projectId.validateSettings(settings);

        final GoogleCSConnectionNodeSettings tmpSettings = new GoogleCSConnectionNodeSettings();
        tmpSettings.loadSettingsFrom(settings);
        tmpSettings.validateDeeper();
    }

    /**
     * Validate current settings values.
     *
     * @throws InvalidSettingsException if the settings are invalid.
     */
    void validateDeeper() throws InvalidSettingsException {
        if (StringUtils.isBlank(m_projectId.getStringValue())) {
            throw new InvalidSettingsException("Project identifier required.");
        }
    }

    /**
     * @param settings the NodeSettingsRO to read from.
     * @throws InvalidSettingsException if the settings are invalid.
     */
    void loadSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_projectId.loadSettingsFrom(settings);
    }

    GoogleCloudStorageConnectionInformation createConnectionInformations(final GoogleApiConnection apiConnection) {
        final GoogleCloudStorageConnectionInformation infos = new GoogleCloudStorageConnectionInformation(apiConnection, m_projectId.getStringValue());
        infos.setProtocol(GoogleCSRemoteFileHandler.PROTOCOL.getName());
        infos.setHost(m_projectId.getStringValue());
        return infos;
    }
}

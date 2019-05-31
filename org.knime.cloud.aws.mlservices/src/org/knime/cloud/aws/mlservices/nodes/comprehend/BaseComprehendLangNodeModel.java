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
 *   May 22, 2019 (Julian Bunzel): created
 */
package org.knime.cloud.aws.mlservices.nodes.comprehend;

import org.knime.cloud.aws.mlservices.utils.comprehend.ComprehendUtils;
import org.knime.cloud.core.util.port.CloudConnectionInformation;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelString;

/**
 * Node model for Amazon Comprehend nodes that need information about the source language of the text. This node model
 * extends the {@link BaseComprehendNodeModel} which captures all the commonality between the implementations.
 *
 * @author Jim Falgout, KNIME AG, Zurich, Switzerland
 */
public abstract class BaseComprehendLangNodeModel extends BaseComprehendNodeModel {

    /** Name of the input text column to analyze */
    private final SettingsModelString m_sourceLanguage = ComprehendUtils.getSourceLanguageModel();

    /**
     * Returns the {@link SettingsModelString} containing the source language.
     *
     * @return Returns the {@link SettingsModelString} containing the source language.
     */
    protected SettingsModelString getSourceLanguage() {
        return m_sourceLanguage;
    }

    @Override
    protected final ComprehendOperation getOperationInstance(final CloudConnectionInformation cxnInfo,
        final DataTableSpec outputSpec, final String textColumn) {
        return getOperationInstance(cxnInfo, outputSpec, textColumn, m_sourceLanguage.getStringValue());
    }

    /**
     * Creates and returns an instance of a specific implementation of {@link ComprehendOperation}.
     *
     * @param cxnInfo The connection information
     * @param outputSpec The output data table spec
     * @param textColumn The name of the text column
     * @param sourceLanguage The source language
     * @return New instance of a specific {@code ComprehendOperation} implementation
     */
    protected abstract ComprehendOperation getOperationInstance(final CloudConnectionInformation cxnInfo,
        final DataTableSpec outputSpec, final String textColumn, final String sourceLanguage);

    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        super.saveSettingsTo(settings);
        m_sourceLanguage.saveSettingsTo(settings);
    }

    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        super.loadValidatedSettingsFrom(settings);
        m_sourceLanguage.loadSettingsFrom(settings);
    }

    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        super.validateSettings(settings);
        m_sourceLanguage.validateSettings(settings);
    }
}
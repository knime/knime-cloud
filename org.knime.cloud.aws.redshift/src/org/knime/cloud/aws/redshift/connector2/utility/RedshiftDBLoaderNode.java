/*
 * ------------------------------------------------------------------------
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
 * ------------------------------------------------------------------------
 */
package org.knime.cloud.aws.redshift.connector2.utility;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableSet;
import static org.knime.database.agent.loader.DBLoaderMode.REMOTE_TEMPORARY_FILE;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.knime.core.data.DataTableSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.defaultnodesettings.SettingsModel;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.database.agent.loader.DBLoaderMode;
import org.knime.database.node.component.dbrowser.SettingsModelDBMetadata;
import org.knime.database.node.io.load.impl.ConnectableCsvLoaderNode;
import org.knime.database.node.io.load.impl.ConnectableCsvLoaderNodeSettings;
import org.knime.database.port.DBDataPortObject;
import org.knime.database.port.DBDataPortObjectSpec;
import org.knime.database.port.DBSessionPortObjectSpec;

/**
 * Implementation of the loader node for the Redshift database.
 *
 * @author Tobias Koetter, KNIME GmbH, Konstanz, Germany
 */
public class RedshiftDBLoaderNode extends ConnectableCsvLoaderNode {
    private static final Set<DBLoaderMode> MODES =
        unmodifiableSet(new LinkedHashSet<>(asList(REMOTE_TEMPORARY_FILE)));

    /**
     * Constructs a {@link RedshiftDBLoaderNode} object.
     */
    public RedshiftDBLoaderNode() {
        super(MODES);
    }

    @Override
    protected DBDataPortObjectSpec configureModel(final PortObjectSpec[] inSpecs,
        final List<SettingsModel> settingsModels, final ConnectableCsvLoaderNodeSettings customSettings)
        throws InvalidSettingsException {
        final DBSessionPortObjectSpec sessionPortObjectSpec = (DBSessionPortObjectSpec)inSpecs[1];
        final SettingsModelDBMetadata tableNameModel = customSettings.getTableNameModel();
        validateColumns(false, createModelConfigurationExecutionMonitor(sessionPortObjectSpec.getDBSession()),
            (DataTableSpec)inSpecs[0], sessionPortObjectSpec, tableNameModel.toDBTable());
        return super.configureModel(inSpecs, settingsModels, customSettings);
    }

    @Override
    protected DBDataPortObject load(final ExecutionParameters<ConnectableCsvLoaderNodeSettings> parameters)
        throws Exception {
        validateColumns(false, parameters.getExecutionContext(), parameters.getRowInput().getDataTableSpec(),
            parameters.getSessionPortObject(), parameters.getCustomSettings().getTableNameModel().toDBTable());
        return super.load(parameters);
    }
}

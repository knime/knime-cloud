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
 *   Jun 22, 2019 (Tobias): created
 */
package org.knime.cloud.aws.redshift.connector2;

import static org.knime.database.connection.DBConnectionManagerAttributes.ATTRIBUTE_JDBC_PROPERTIES;
import static org.knime.database.node.component.attribute.Attributes.saveAttributeValues;
import static org.knime.database.node.connector.generic.DBConnectorNodeMigrationRule.extractAttributeValues;
import static org.knime.database.node.connector.generic.DBConnectorNodeMigrationRule.migrateCommonLegacyServerConnectorSettings;
import static org.knime.database.util.DerivableProperties.ValueType.LITERAL;
import static org.knime.workflow.migration.util.NodeSettingsMigrationUtilities.getOrAddNodeSettings;

import java.io.Serializable;
import java.util.Map;

import org.knime.cloud.aws.redshift.connector.RedshiftConnectorNodeFactory;
import org.knime.cloud.aws.redshift.connector2.utility.Redshift;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.database.DBType;
import org.knime.database.dialect.impl.SQL92DBSQLDialect;
import org.knime.database.util.DerivableProperties;
import org.knime.workflow.migration.MigrationException;
import org.knime.workflow.migration.MigrationNodeMatchResult;
import org.knime.workflow.migration.NodeMigrationAction;
import org.knime.workflow.migration.NodeMigrationRule;
import org.knime.workflow.migration.NodeSettingsMigrationManager;
import org.knime.workflow.migration.model.MigrationNode;

/**
 * Node migration rule for the <em>Redshift Connector</em> node.
 * @author Tobias Koetter, KNIME GmbH, Konstanz, Germany
 */
public class RedshiftDBConnectorNodeMigrationRule extends NodeMigrationRule {

    /**
     * {@inheritDoc}
     */
    @Override
    protected Class<? extends NodeFactory<?>> getReplacementNodeFactoryClass(final MigrationNode migrationNode,
        final MigrationNodeMatchResult matchResult) {
        return RedshiftDBConnectorNodeFactory.class;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected MigrationNodeMatchResult match(final MigrationNode migrationNode) {
        return MigrationNodeMatchResult.of(migrationNode,
            RedshiftConnectorNodeFactory.class.equals(migrationNode.getOriginalNodeFactoryClass()) ?
                NodeMigrationAction.REPLACE : null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void migrate(final MigrationNode migrationNode, final MigrationNodeMatchResult matchResult)
        throws MigrationException {
        try {
            final DBType dbType = Redshift.DB_TYPE;
            final NodeSettingsMigrationManager settingsManager = createSettingsManager(migrationNode);
            migrateCommonLegacyServerConnectorSettings(settingsManager, "redshift-connection", dbType);
            final NodeSettingsRO legacyModelSettings = settingsManager.getSourceModelSettings();
            final NodeSettingsWO newModelSettings = settingsManager.getDestinationModelSettings();
            final Map<String, Serializable> attributeValues = extractAttributeValues(legacyModelSettings);
            //parameters are stored in the redshift config section
            final NodeSettingsRO legacyConfig = legacyModelSettings.getNodeSettings("redshift-connection");
            final DerivableProperties jdbcProperties = ATTRIBUTE_JDBC_PROPERTIES.getDefaultValue().copy();
            if (legacyConfig.containsKey("parameter")) {
                String parameter = legacyConfig.getString("parameter", "");
                if (parameter.startsWith("?")) {
                    //remove leading ?
                    parameter = parameter.substring(1);
                }
                final String[] keyValues = parameter.split(";");
                for (String kv : keyValues) {
                    final String[] keyValue = kv.split("=");
                    if (keyValue.length == 2) {
                        jdbcProperties.setDerivableProperty(keyValue[0], LITERAL, keyValue[1]);
                    }
                }
            }
            if (legacyModelSettings.getNodeSettings("redshift-connection").getBoolean("useSsl")) {
                jdbcProperties.setDerivableProperty("ssl", LITERAL, String.valueOf(true));
                jdbcProperties.setDerivableProperty("sslfactory", LITERAL,
                    "com.amazon.redshift.ssl.NonValidatingFactory");
            }
            attributeValues.put(ATTRIBUTE_JDBC_PROPERTIES.getId(), jdbcProperties);
            //The former Redshift StatementManipulator only quoted if the identifier contained a space
            attributeValues.put(SQL92DBSQLDialect.ATTRIBUTE_SYNTAX_IDENTIFIER_DELIMITING_ONLY_SPACES.getId(), true);
            saveAttributeValues(attributeValues, getOrAddNodeSettings(newModelSettings, "session_info", "attributes"));
        } catch (final InvalidSettingsException invalidSettingsException) {
            throw new MigrationException(invalidSettingsException.getMessage(), invalidSettingsException);
        }
        associateEveryOriginalPortWithNew(migrationNode);
    }

}

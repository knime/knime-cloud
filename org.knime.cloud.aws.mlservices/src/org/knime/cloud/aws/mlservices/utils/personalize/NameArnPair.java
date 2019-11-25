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
 *   Nov 2, 2019 (Simon Schmid, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.cloud.aws.mlservices.utils.personalize;

import java.util.AbstractMap.SimpleEntry;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.config.Config;

/**
 * An object of this class holds a pair of a name and an ARN (Amazon Resource Name). It can be used by nodes to store a
 * nice representation of an ARN which can be display in the dialog while having the full ARN at hand.
 *
 * @author Simon Schmid, KNIME GmbH, Konstanz, Germany
 */
public final class NameArnPair extends SimpleEntry<String, String> {

    /** */
    private static final long serialVersionUID = 1L;

    /**
     * @param key
     * @param value
     */
    public NameArnPair(final String key, final String value) {
        super(key, value);
    }

    /**
     * @return the name
     */
    public String getName() {
        return getKey();
    }

    /**
     * @return the ARN
     */
    public String getARN() {
        return getValue();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return getKey();
    }

    /**
     * Loads the settings from the node settings object.
     *
     * @param settings a node settings object
     * @param configKey config key
     * @return an object of this kind with loaded values
     * @throws InvalidSettingsException if some settings are missing
     */
    public static NameArnPair loadSettings(final NodeSettingsRO settings, final String configKey)
        throws InvalidSettingsException {
        final Config config = settings.getConfig(configKey);
        final String name = config.getString("name");
        final String arn = config.getString("arn");
        if (name == null && arn == null) {
            return null;
        }
        return new NameArnPair(name, arn);
    }

    /**
     * Loads the settings from the node settings object.
     *
     * @param settings a node settings object
     * @param configKey config key
     * @param def default value
     * @return an object of this kind with loaded values
     */
    public static NameArnPair loadSettingsForDialog(final NodeSettingsRO settings, final String configKey,
        final NameArnPair def) {
        try {
            return loadSettings(settings, configKey);
        } catch (InvalidSettingsException e) {
            return def;
        }
    }

    /**
     * Saves the settings into the node settings object.
     *
     * @param settings a node settings object
     * @param configKey config key
     * @param nameArnPair the {@link NameArnPair} to save
     */
    public static void saveSettings(final NodeSettingsWO settings, final String configKey,
        final NameArnPair nameArnPair) {
        final Config config = settings.addConfig(configKey);
        config.addString("name", nameArnPair != null ? nameArnPair.getKey() : null);
        config.addString("arn", nameArnPair != null ? nameArnPair.getValue() : null);
    }

    /**
     * @param name the name
     * @param arn the arn
     * @return the created {@link NameArnPair}
     */
    public static NameArnPair of(final String name, final String arn) {
        return new NameArnPair(name, arn);
    }

}

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
 *   Oct 30, 2019 (Simon Schmid, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.cloud.aws.mlservices.nodes.personalize.upload.interaction;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaBuilder.FieldAssembler;
import org.knime.cloud.aws.mlservices.nodes.personalize.upload.AbstractAmazonPersonalizeDataUploadNodeModel;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.port.PortObject;

/**
 * Node model for Amazon Personalize interaction data upload node.
 *
 * @author Simon Schmid, KNIME GmbH, Konstanz, Germany
 */
final class AmazonPersonalizeUploadInteractionDataNodeModel
    extends AbstractAmazonPersonalizeDataUploadNodeModel<AmazonPersonalizeUploadInteractionDataNodeSettings> {

    private static final String EVENT_VALUE = "EVENT_VALUE";

    private static final String EVENT_TYPE = "EVENT_TYPE";

    private static final String TIMESTAMP = "TIMESTAMP";

    private static final String ITEM_ID = "ITEM_ID";

    private static final String USER_ID = "USER_ID";

    static final String DATATYPE = "INTERACTIONS";

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] execute(final PortObject[] inObjects, final ExecutionContext exec) throws Exception {
        if (((BufferedDataTable)inObjects[1]).size() < 1000) {
            throw new IllegalArgumentException("The input table must have at least 1000 rows.");
        }
        return super.execute(inObjects, exec);
    }

    @Override
    protected BufferedDataTable renameColumns(final BufferedDataTable table, final ExecutionContext exec)
        throws CanceledExecutionException {
        final DataTableSpec spec = table.getSpec();
        final DataColumnSpec[] colSpecs = new DataColumnSpec[spec.getNumColumns()];
        int j = 0;
        for (int i = 0; i < spec.getNumColumns(); i++) {
            final DataColumnSpec colSpec = spec.getColumnSpec(i);
            if (colSpec.getName().equals(m_settings.getUserIDColumnName())) {
                final DataColumnSpecCreator dataColumnSpecCreator = new DataColumnSpecCreator(colSpec);
                dataColumnSpecCreator.setName(USER_ID);
                colSpecs[i] = dataColumnSpecCreator.createSpec();
            } else if (colSpec.getName().equals(m_settings.getItemIDColumnName())) {
                final DataColumnSpecCreator dataColumnSpecCreator = new DataColumnSpecCreator(colSpec);
                dataColumnSpecCreator.setName(ITEM_ID);
                colSpecs[i] = dataColumnSpecCreator.createSpec();
            } else if (colSpec.getName().equals(m_settings.getTimestampColumnName())) {
                final DataColumnSpecCreator dataColumnSpecCreator = new DataColumnSpecCreator(colSpec);
                dataColumnSpecCreator.setName(TIMESTAMP);
                colSpecs[i] = dataColumnSpecCreator.createSpec();
            } else if (colSpec.getName().equals(m_settings.getEventTypeColumnName())) {
                final DataColumnSpecCreator dataColumnSpecCreator = new DataColumnSpecCreator(colSpec);
                dataColumnSpecCreator.setName(EVENT_TYPE);
                colSpecs[i] = dataColumnSpecCreator.createSpec();
            } else if (colSpec.getName().equals(m_settings.getEventValueColumnName())) {
                final DataColumnSpecCreator dataColumnSpecCreator = new DataColumnSpecCreator(colSpec);
                dataColumnSpecCreator.setName(EVENT_VALUE);
                colSpecs[i] = dataColumnSpecCreator.createSpec();
            } else {
                final DataColumnSpecCreator dataColumnSpecCreator = new DataColumnSpecCreator(colSpec);
                dataColumnSpecCreator.setName(PREFIX_METADATA_FIELD + j++);
                colSpecs[i] = dataColumnSpecCreator.createSpec();
            }
        }
        return exec.createSpecReplacerTable(table, new DataTableSpec(colSpecs));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected AmazonPersonalizeUploadInteractionDataNodeSettings getSettings() {
        if (m_settings != null) {
            return m_settings;
        }
        return new AmazonPersonalizeUploadInteractionDataNodeSettings();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected String getDatasetType() {
        return DATATYPE;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected FieldAssembler<Schema> createFieldAssembler(final String namespace) {
        FieldAssembler<Schema> fieldAssembler = SchemaBuilder.record("Interactions").namespace(namespace).fields()//
            .requiredString(USER_ID)//
            .requiredString(ITEM_ID)//
            .requiredLong(TIMESTAMP);
        if (m_settings.getEventTypeColumnName() != null) {
            fieldAssembler = fieldAssembler.requiredString(EVENT_TYPE);
        }
        if (m_settings.getEventValueColumnName() != null) {
            fieldAssembler = fieldAssembler.requiredFloat(EVENT_VALUE);
        }
        return fieldAssembler;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected String getSchemaNamePrefix() {
        String schemaNamePrefix = super.getSchemaNamePrefix();
        if (m_settings.getEventTypeColumnName() != null) {
            schemaNamePrefix += "-" + EVENT_TYPE;
        }
        if (m_settings.getEventValueColumnName() != null) {
            schemaNamePrefix += "- " + EVENT_VALUE;
        }
        return schemaNamePrefix;
    }
}

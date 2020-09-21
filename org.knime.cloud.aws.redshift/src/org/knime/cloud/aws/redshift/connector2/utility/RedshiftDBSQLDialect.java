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
 *   Jun 10, 2019 (Tobias): created
 */
package org.knime.cloud.aws.redshift.connector2.utility;

import java.util.Objects;

import org.knime.database.attribute.AttributeCollection;
import org.knime.database.attribute.AttributeCollection.Accessibility;
import org.knime.database.dialect.DBSQLDialect;
import org.knime.database.dialect.DBSQLDialectFactory;
import org.knime.database.dialect.DBSQLDialectFactoryParameters;
import org.knime.database.dialect.DBSQLDialectParameters;
import org.knime.database.dialect.impl.SQL92DBSQLDialect;
import org.knime.database.extension.postgres.dialect.PostgreSQLDBSQLDialect;

/**
 * Amazon Redshift SQL dialect implementation based on the {@link PostgreSQLDBSQLDialect} class.
 *
 * @author Tobias Koetter, KNIME GmbH, Konstanz, Germany
 */
public class RedshiftDBSQLDialect extends PostgreSQLDBSQLDialect {

    /**
     * {@link DBSQLDialectFactory} that produces {@link RedshiftDBSQLDialect} instances.
     *
     * @author Tobias Koetter, KNIME GmbH, Konstanz, Germany
     */
    public static class Factory implements DBSQLDialectFactory {
        @Override
        public DBSQLDialect createDialect(final DBSQLDialectFactoryParameters parameters) {
            return new RedshiftDBSQLDialect(this,
                new DBSQLDialectParameters(Objects.requireNonNull(parameters, "parameters").getSessionReference()));
        }

        @Override
        public AttributeCollection getAttributes() {
            return ATTRIBUTES;
        }

        @Override
        public String getDescription() {
            return DESCRIPTION;
        }

        @Override
        public String getId() {
            return ID;
        }

        @Override
        public String getName() {
            return NAME;
        }
    }

    /**
     * The {@link AttributeCollection} of this {@link DBSQLDialect}.
     *
     * @see Factory#getAttributes()
     */
    @SuppressWarnings("hiding")
    public static final AttributeCollection ATTRIBUTES;

    static {
        final AttributeCollection.Builder builder = AttributeCollection.builder(SQL92DBSQLDialect.ATTRIBUTES);
        // Capabilities
        builder.setGroup(SQL92DBSQLDialect.ATTRIBUTE_GROUP_CAPABILITIES);

        builder.add(Accessibility.HIDDEN,
            SQL92DBSQLDialect.ATTRIBUTE_CAPABILITY_DEFINE_CREATE_TABLE_CONSTRAINT_NAME, true);

        builder.add(Accessibility.HIDDEN,
            SQL92DBSQLDialect.ATTRIBUTE_CAPABILITY_DROP_TABLE, true);

        builder.add(Accessibility.HIDDEN,
            SQL92DBSQLDialect.ATTRIBUTE_CAPABILITY_TABLE_REFERENCE_DERIVED_TABLE, true);

        builder.add(Accessibility.HIDDEN,
            SQL92DBSQLDialect.ATTRIBUTE_CAPABILITY_INSERT_AS_SELECT, true);

        builder.add(Accessibility.HIDDEN,
            SQL92DBSQLDialect.ATTRIBUTE_CAPABILITY_EXPRESSION_CASE, true);

        builder.add(Accessibility.HIDDEN,
            SQL92DBSQLDialect.ATTRIBUTE_CAPABILITY_MINUS_OPERATION, true);

        // Etc.
        builder.setGroup(null);

        builder.add(Accessibility.HIDDEN,
            SQL92DBSQLDialect.ATTRIBUTE_SYNTAX_CREATE_TABLE_IF_NOT_EXISTS, "IF NOT EXISTS");

        builder.add(Accessibility.HIDDEN,
            SQL92DBSQLDialect.ATTRIBUTE_SYNTAX_TABLE_REFERENCE_KEYWORD, "AS");

        builder.add(Accessibility.HIDDEN,
            SQL92DBSQLDialect.ATTRIBUTE_SYNTAX_CREATE_TABLE_TEMPORARY, "TEMPORARY");

        builder.add(Accessibility.HIDDEN,
            SQL92DBSQLDialect.ATTRIBUTE_SYNTAX_MINUS_OPERATOR_KEYWORD, "EXCEPT");

        ATTRIBUTES = builder.build();
    }

    /**
     * The {@linkplain #getId() ID} of the {@link PostgreSQLDBSQLDialect} instances.
     *
     * @see DBSQLDialectFactory#getId()
     * @see RedshiftDBSQLDialect.Factory#getId()
     */
    @SuppressWarnings("hiding")
    public static final String ID = "redshift";

    /**
     * The {@linkplain #getDescription() description} of the {@link PostgreSQLDBSQLDialect} instances.
     *
     * @see DBSQLDialectFactory#getDescription()
     * @see PostgreSQLDBSQLDialect.Factory#getDescription()
     */
    static final String DESCRIPTION = "Amazon Redshift";

    /**
     * The {@linkplain #getName() name} of the {@link PostgreSQLDBSQLDialect} instances.
     *
     * @see DBSQLDialectFactory#getName()
     * @see PostgreSQLDBSQLDialect.Factory#getName()
     */
    static final String NAME = "Amazon Redshift";

    /**
     * @param factory
     * @param dialectParameters
     */
    protected RedshiftDBSQLDialect(final DBSQLDialectFactory factory, final DBSQLDialectParameters dialectParameters) {
        super(factory, dialectParameters);
    }

}

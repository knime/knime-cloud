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
 *   Jun 15, 2018 (jtyler): created
 */
package org.knime.cloud.google.util;

import java.io.IOException;
import java.security.GeneralSecurityException;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.knime.cloud.core.util.port.CloudConnectionInformation;
import org.knime.cloud.google.drive.filehandler.GoogleDriveRemoteFileHandler;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.ModelContentRO;
import org.knime.core.node.ModelContentWO;
import org.knime.core.node.util.CheckUtils;
import org.knime.google.api.data.GoogleApiConnection;

/**
 * TODO (short) javadoc comment
 * @author jtyler
 */
public class GoogleDriveConnectionInformation extends CloudConnectionInformation {

    private static final long serialVersionUID = 1L;

    private final GoogleApiConnection m_connection;

    /**
     * @param connection non-null connection object.
     *
     */
    public GoogleDriveConnectionInformation(final GoogleApiConnection connection) {
        m_connection = CheckUtils.checkArgumentNotNull(connection);
        setProtocol(GoogleDriveRemoteFileHandler.PROTOCOL.getName());
        setHost("google-drive-api");
        setUser("user");
    }

    /**
     * @param model
     * @throws InvalidSettingsException
     * @throws IOException
     * @throws GeneralSecurityException
     */
    public GoogleDriveConnectionInformation(final ModelContentRO model) throws InvalidSettingsException, GeneralSecurityException, IOException {
        super(model);
        m_connection = new GoogleApiConnection(model);
    }

    @Override
    public void save(final ModelContentWO model) {
        super.save(model);
        m_connection.save(model);
    }

    public static GoogleDriveConnectionInformation load(final ModelContentRO model) throws InvalidSettingsException {
        try {
            return new GoogleDriveConnectionInformation(model);
        } catch (GeneralSecurityException | IOException e) {
            throw new InvalidSettingsException(e.getLocalizedMessage());
        }
    }

    /**
     * @return the connection
     */
    public GoogleApiConnection getGoogleConnection() {
        return m_connection;
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        final GoogleDriveConnectionInformation rhs = (GoogleDriveConnectionInformation)obj;

        return new EqualsBuilder().appendSuper(super.equals(obj)).append(m_connection, rhs.m_connection).isEquals();
    }

    @Override
    public int hashCode() {
        final HashCodeBuilder hashBuilder = new HashCodeBuilder();
        return hashBuilder.appendSuper(super.hashCode()).append(m_connection).toHashCode();
    }
}

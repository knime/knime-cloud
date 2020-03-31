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
 *   20.08.2019 (Mareike Hoeger, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.cloud.aws.filehandling.connections;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Objects;

import org.knime.cloud.core.util.port.CloudConnectionInformation;
import org.knime.core.node.util.FileSystemBrowser;
import org.knime.filehandling.core.connections.FSConnection;
import org.knime.filehandling.core.connections.FSFileSystem;
import org.knime.filehandling.core.filechooser.NioFileSystemBrowser;

import com.amazonaws.ClientConfiguration;

/**
 * The Amazon S3 implementation of the {@link FSConnection} interface.
 *
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
public class S3FSConnection implements FSConnection {

    private final CloudConnectionInformation m_connInfo;

    private final S3FileSystemProvider m_provider;

    private final long m_cacheTimeToLive = 60000;

    private final boolean m_normalizePaths = true;

    /**
     * Creates a new {@link S3FSConnection} for the given connection information.
     *
     * @param connectionInformation the cloud connection information
     */
    public S3FSConnection(final CloudConnectionInformation connectionInformation) {
        this(connectionInformation, new ClientConfiguration());
    }

    /**
     * Creates a new {@link S3FSConnection} for the given connection information.
     *
     * @param connectionInformation the cloud connection information
     * @param clientConfig the {@link ClientConfiguration} to use
     */
    public S3FSConnection(final CloudConnectionInformation connectionInformation,
        final ClientConfiguration clientConfig) {
        Objects.requireNonNull(connectionInformation);
        m_connInfo = connectionInformation;
        m_provider = new S3FileSystemProvider(clientConfig, m_connInfo.toURI(), m_cacheTimeToLive, m_normalizePaths);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public FSFileSystem<?> getFileSystem() {
        final HashMap<String, CloudConnectionInformation> env = new HashMap<>();
        env.put(S3FileSystemProvider.CONNECTION_INFORMATION, m_connInfo);

        final URI uri = m_connInfo.toURI();
        try {
            return m_provider.getOrCreateFileSystem(uri, env);
        } catch (final IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    /**
     * Closes the FileSystem for this connection
     *
     * @throws IOException if an I/O error occurs
     */
    public void closeFileSystem() throws IOException {
        final URI uri = m_connInfo.toURI();
        if (m_provider.isOpen(uri)) {
            m_provider.getFileSystem(uri).close();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public FileSystemBrowser getFileSystemBrowser() {
        return new NioFileSystemBrowser(this);
    }

}

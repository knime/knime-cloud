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
package org.knime.cloud.aws.filehandling.s3.fs;

import java.util.Map;

import org.knime.cloud.aws.filehandling.s3.node.S3ConnectorNodeSettings;
import org.knime.cloud.aws.filehandling.s3.uriexporter.S3SignedURIExporterFactory;
import org.knime.cloud.aws.filehandling.s3.uriexporter.S3URIExporterFactory;
import org.knime.cloud.core.util.port.CloudConnectionInformation;
import org.knime.core.node.util.CheckUtils;
import org.knime.core.node.util.FileSystemBrowser;
import org.knime.core.node.workflow.CredentialsProvider;
import org.knime.filehandling.core.connections.FSConnection;
import org.knime.filehandling.core.connections.FSFileSystem;
import org.knime.filehandling.core.connections.uriexport.URIExporterFactory;
import org.knime.filehandling.core.connections.uriexport.URIExporterFactoryMapBuilder;
import org.knime.filehandling.core.connections.uriexport.URIExporterID;
import org.knime.filehandling.core.connections.uriexport.URIExporterIDs;
import org.knime.filehandling.core.filechooser.NioFileSystemBrowser;

/**
 * The Amazon S3 implementation of the {@link FSConnection} interface.
 *
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
public class S3FSConnection implements FSConnection {

    private static final Map<URIExporterID, URIExporterFactory> URI_EXPORTER_FACTORIES =
        new URIExporterFactoryMapBuilder() //
            .add(URIExporterIDs.DEFAULT, S3URIExporterFactory.getInstance()) //
            .add(URIExporterIDs.DEFAULT_HADOOP, S3URIExporterFactory.getInstance()) //
            .add(S3URIExporterFactory.EXPORTER_ID, S3URIExporterFactory.getInstance()) //
            .add(S3SignedURIExporterFactory.EXPORTER_ID, S3SignedURIExporterFactory.getInstance()) //
            .build();

    private static final long CACHE_TTL_MILLIS = 6000;

    private final S3FileSystem m_fileSystem;

    /**
     * Creates a new {@link S3FSConnection} for the given connection information and settings.
     *
     * @param connInfo the cloud connection information
     * @param settings the node settings
     * @param credentials The credentials provider.
     */
    public S3FSConnection(final CloudConnectionInformation connInfo, final S3ConnectorNodeSettings settings,
        final CredentialsProvider credentials) {

        CheckUtils.checkArgumentNotNull(connInfo, "CloudConnectionInformation must not be null");
        CheckUtils.checkArgumentNotNull(settings, "S3ConnectorNodeSettings must not be null");

        m_fileSystem = new S3FileSystem(connInfo, settings, CACHE_TTL_MILLIS, credentials);
    }

    /**
     * Creates a new {@link S3FSConnection} for the given connection information.
     *
     * @param connInfo the cloud connection information
     */
    public S3FSConnection(final CloudConnectionInformation connInfo) {
        this(connInfo, new S3ConnectorNodeSettings(), null);
    }

    @Override
    public FSFileSystem<?> getFileSystem() {
        return m_fileSystem;
    }

    @Override
    public FileSystemBrowser getFileSystemBrowser() {
        return new NioFileSystemBrowser(this);
    }

    @Override
    public Map<URIExporterID, URIExporterFactory> getURIExporterFactories() {
        return URI_EXPORTER_FACTORIES;
    }
}

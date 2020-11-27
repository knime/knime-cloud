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

import java.io.IOException;
import java.util.Map;

import org.knime.cloud.aws.filehandling.s3.node.S3ConnectorNodeSettings;
import org.knime.cloud.core.util.port.CloudConnectionInformation;
import org.knime.core.node.util.CheckUtils;
import org.knime.core.node.util.FileSystemBrowser;
import org.knime.filehandling.core.connections.FSConnection;
import org.knime.filehandling.core.connections.FSFileSystem;
import org.knime.filehandling.core.connections.uriexport.URIExporter;
import org.knime.filehandling.core.connections.uriexport.URIExporterID;
import org.knime.filehandling.core.connections.uriexport.URIExporterIDs;
import org.knime.filehandling.core.connections.uriexport.URIExporterMapBuilder;
import org.knime.filehandling.core.filechooser.NioFileSystemBrowser;

/**
 * The Amazon S3 implementation of the {@link FSConnection} interface.
 *
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
public class S3FSConnection implements FSConnection {

    private static final Map<URIExporterID, URIExporter> URI_EXPORTERS = new URIExporterMapBuilder() //
            .add(URIExporterIDs.DEFAULT, S3URIExporter.getInstance()) //
            .add(URIExporterIDs.DEFAULT_HADOOP, S3URIExporter.getInstance()) //
            .build();

    private static final long CACHE_TTL_MILLIS = 6000;

    private final S3FileSystem m_fileSystem;

    /**
     * Creates a new {@link S3FSConnection} for the given connection information.
     *
     * @param connInfo the cloud connection information
     * @param settings the node settings
     * @throws IOException
     */
    public S3FSConnection(final CloudConnectionInformation connInfo,
        final S3ConnectorNodeSettings settings) throws IOException {

        CheckUtils.checkArgumentNotNull(connInfo, "CloudConnectionInformation must not be null");
        CheckUtils.checkArgumentNotNull(settings, "S3ConnectorNodeSettings must not be null");


        m_fileSystem = new S3FileSystem(connInfo, settings, CACHE_TTL_MILLIS);
    }

    /**
     * @param connInfo the cloud connection information
     * @throws IOException
     */
    public S3FSConnection(final CloudConnectionInformation connInfo) throws IOException {
        this(connInfo, new S3ConnectorNodeSettings());
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
    public Map<URIExporterID, URIExporter> getURIExporters() {
        return URI_EXPORTERS;
    }
}

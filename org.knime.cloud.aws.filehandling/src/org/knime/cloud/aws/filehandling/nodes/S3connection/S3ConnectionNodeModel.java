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
package org.knime.cloud.aws.filehandling.nodes.S3connection;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;

import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObjectSpec;
import org.knime.cloud.aws.filehandling.connections.S3Connection;
import org.knime.cloud.aws.util.AmazonConnectionInformationPortObject;
import org.knime.cloud.aws.util.AmazonConnectionInformationPortObject.Serializer;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettings;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortObjectSpecZipInputStream;
import org.knime.core.node.port.PortObjectSpecZipOutputStream;
import org.knime.core.node.port.PortObjectZipInputStream;
import org.knime.core.node.port.PortObjectZipOutputStream;
import org.knime.core.node.port.PortType;
import org.knime.core.node.port.PortUtil;
import org.knime.core.node.workflow.FSConnectionNode;
import org.knime.filehandling.core.connections.FSConnectionRegistry;
import org.knime.filehandling.core.port.FileSystemPortObject;
import org.knime.filehandling.core.port.FileSystemPortObjectSpec;

/**
 *
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
public class S3ConnectionNodeModel extends NodeModel implements FSConnectionNode {

    private static final String FN_FILE_SYSTEM = "fileSystem";
    private static final String FN_CONN_INFO_SPEC = "connectionInfoSpec";
    private static final String FN_CONN_INFO = "connectionInfo";
    private static final String CFG_FILE_SYSTEM_ID = "fileSystemId";

    private static final String FILE_SYSTEM_NAME = "S 3";

    private static final NodeLogger LOGGER = NodeLogger.getLogger(S3ConnectionNodeModel.class);

    private S3Connection m_fsConn;

    private String m_fsId;

    private AmazonConnectionInformationPortObject m_awsConnectionInfo;

    /**
     * The NodeModel for the S3Connection node
     */
    public S3ConnectionNodeModel() {
    	super(new PortType[]{AmazonConnectionInformationPortObject.TYPE}, new PortType[]{FileSystemPortObject.TYPE});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] execute(final PortObject[] inObjects, final ExecutionContext exec) throws Exception {
        m_awsConnectionInfo = (AmazonConnectionInformationPortObject)inObjects[0];


        //TODO: Test if connection is available and if switch role is required!!!

//        final CloudConnectionInformation conInfo = m_awsConnectionInfo.getConnectionInformation();
//        URI resolve = conInfo.toURI().resolve("/");
//        try {
//            RemoteFile<S3Connection> s3RemoteFile = RemoteFileFactory.createRemoteFile(resolve, conInfo,
//                new ConnectionMonitor<S3Connection>());
//            S3Connection connection = s3RemoteFile.getConnection();
//            if (connection.restrictedPermissions()) {
//                setWarningMessage("The credentials provided have restricted permissions. "
//                    + "File browsing in the Remote File nodes might not work as expected.\n"
//                    + "All buckets will be assumed existing, as they cannot be listed.");
//            }
//        } catch (InvalidSettingsException ex) {
//            throw ex;
//        } catch (Exception ex) {
//            throw new InvalidSettingsException(ex.getMessage());
//        }

        m_fsConn = new S3Connection(m_awsConnectionInfo.getConnectionInformation());
        FSConnectionRegistry.getInstance().register(m_fsId, m_fsConn);
        return new PortObject[]{new FileSystemPortObject(createSpec())};
    }


    @Override
    protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs) {
        m_fsId = FSConnectionRegistry.getInstance().getKey();
        return new PortObjectSpec[]{createSpec()};
    }

    private FileSystemPortObjectSpec createSpec() {
        return new FileSystemPortObjectSpec(FILE_SYSTEM_NAME, m_fsId);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadInternals(final File nodeInternDir, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {
        final File settingsFile = new File(nodeInternDir, FN_FILE_SYSTEM);
        try (InputStream inStream = Files.newInputStream(settingsFile.toPath())) {
            final NodeSettingsRO settings = NodeSettings.loadFromXML(inStream);
            m_fsId = settings.getString(CFG_FILE_SYSTEM_ID);
        }  catch (InvalidSettingsException e) {
            throw new IOException(e);
        }

        final org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObjectSpec
            .Serializer specSerializer = new ConnectionInformationPortObjectSpec.Serializer();
        final Serializer serializer = new AmazonConnectionInformationPortObject.Serializer();
        final File specFile = new File(nodeInternDir, FN_CONN_INFO_SPEC);
        if (!specFile.exists()) {
            //nothing to load
            return;
        }
        final File file = new File(nodeInternDir, FN_CONN_INFO);
        try (@SuppressWarnings("resource")
            PortObjectSpecZipInputStream specIn =
                PortUtil.getPortObjectSpecZipInputStream(new BufferedInputStream(new FileInputStream(specFile)));
            @SuppressWarnings("resource")
            PortObjectZipInputStream in =
                PortUtil.getPortObjectZipInputStream(new BufferedInputStream(new FileInputStream(file)));) {
            final ConnectionInformationPortObjectSpec spec = specSerializer.loadPortObjectSpec(specIn);
            m_awsConnectionInfo = serializer.loadPortObject(in, spec, exec);
            m_fsConn = new S3Connection(m_awsConnectionInfo.getConnectionInformation());
            FSConnectionRegistry.getInstance().register(m_fsId, m_fsConn);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveInternals(final File nodeInternDir, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {
        final NodeSettings settings = new NodeSettings(FN_FILE_SYSTEM);
        settings.addString(CFG_FILE_SYSTEM_ID, m_fsId);
        final File settingsFile = new File(nodeInternDir, FN_FILE_SYSTEM);
        try (OutputStream outStream = Files.newOutputStream(settingsFile.toPath())) {
            settings.saveToXML(outStream);
        }

        if (m_awsConnectionInfo == null) {
            //nothing to save
            return;
        }
        final org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObjectSpec
        .Serializer specSerializer = new ConnectionInformationPortObjectSpec.Serializer();
        final Serializer serializer = new AmazonConnectionInformationPortObject.Serializer();
        final File specFile = new File(nodeInternDir, FN_CONN_INFO_SPEC);
        final File file = new File(nodeInternDir, FN_CONN_INFO);
        try (@SuppressWarnings("resource")
            PortObjectSpecZipOutputStream specOut = PortUtil.getPortObjectSpecZipOutputStream(
                new BufferedOutputStream(new FileOutputStream(specFile)));
            @SuppressWarnings("resource")
            PortObjectZipOutputStream out =
                PortUtil.getPortObjectZipOutputStream(new BufferedOutputStream(new FileOutputStream(file)));) {
            specSerializer.savePortObjectSpec((ConnectionInformationPortObjectSpec)m_awsConnectionInfo.getSpec(),
                specOut);
            serializer.savePortObject(m_awsConnectionInfo, out, exec);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        //nothing to do
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        //nothing to do
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        //nothing to do
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void onDispose() {
        //close the file system also when the workflow is closed
        reset();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void reset() {
        try {
            if (m_fsConn != null) {
                m_fsConn.closeFileSystem();
            }
        } catch (final IOException ex) {
            LOGGER.error("Exception closing file system: " + ex.getMessage(), ex);
        } finally {
            FSConnectionRegistry.getInstance().deregister(m_fsId);
            m_awsConnectionInfo = null;
            m_fsId = null;
            m_fsConn = null;
        }
    }
}

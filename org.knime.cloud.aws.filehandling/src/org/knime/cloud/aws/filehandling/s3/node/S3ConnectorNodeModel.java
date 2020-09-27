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
package org.knime.cloud.aws.filehandling.s3.node;

import java.io.File;
import java.io.IOException;
import java.util.Objects;

import org.knime.cloud.aws.filehandling.s3.fs.S3FSConnection;
import org.knime.cloud.aws.filehandling.s3.fs.S3FileSystem;
import org.knime.cloud.aws.util.AmazonConnectionInformationPortObject;
import org.knime.cloud.core.util.port.CloudConnectionInformation;
import org.knime.cloud.core.util.port.CloudConnectionInformationPortObjectSpec;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.filehandling.core.connections.FSConnectionRegistry;
import org.knime.filehandling.core.connections.FSLocationSpec;
import org.knime.filehandling.core.port.FileSystemPortObject;
import org.knime.filehandling.core.port.FileSystemPortObjectSpec;

import software.amazon.awssdk.awscore.exception.AwsServiceException;

/**
 *
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
public class S3ConnectorNodeModel extends NodeModel {

    private static final String FILE_SYSTEM_NAME = "Amazon S3";

    private S3ConnectorNodeSettings m_settings = new S3ConnectorNodeSettings();

    private S3FSConnection m_fsConn;

    private String m_fsId;

    private AmazonConnectionInformationPortObject m_awsConnectionInfo;

    /**
     * The NodeModel for the S3Connection node
     */
    public S3ConnectorNodeModel() {
        super(new PortType[]{AmazonConnectionInformationPortObject.TYPE}, new PortType[]{FileSystemPortObject.TYPE});
    }

    @Override
    protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs) {
        m_fsId = FSConnectionRegistry.getInstance().getKey();

        final CloudConnectionInformation conInfo =
            ((CloudConnectionInformationPortObjectSpec)inSpecs[0]).getConnectionInformation();
        return new PortObjectSpec[]{createSpec(conInfo)};
    }

    private FileSystemPortObjectSpec createSpec(final CloudConnectionInformation conInfo) {
        final FSLocationSpec fsLocationSpec = S3FileSystem.createFSLocationSpec(conInfo);
        return new FileSystemPortObjectSpec(FILE_SYSTEM_NAME, m_fsId, fsLocationSpec);
    }

    @Override
    protected PortObject[] execute(final PortObject[] inObjects, final ExecutionContext exec) throws Exception {
        m_awsConnectionInfo = (AmazonConnectionInformationPortObject)inObjects[0];

        //TODO: Test if connection is available and if switch role is required!!!

        final CloudConnectionInformation conInfo = m_awsConnectionInfo.getConnectionInformation();
        m_fsConn = new S3FSConnection(conInfo, m_settings);
        FSConnectionRegistry.getInstance().register(m_fsId, m_fsConn);

        if (conInfo.isUseAnonymous()) {
            setWarningMessage("You are using anonymous credentials." + "File browsing might not work as expected.\n"
                + "Browsing will only be available when bucket name is given in file/URL field.");
        } else {
            testFileSystemConnection(m_fsConn);
        }
        return new PortObject[]{new FileSystemPortObject(createSpec(conInfo))};
    }

    @SuppressWarnings("resource")
    private void testFileSystemConnection(final S3FSConnection fsConn) throws InvalidSettingsException {
        final S3FileSystem fileSystem = (S3FileSystem)fsConn.getFileSystem();
        try {
            fileSystem.getClient().listBuckets();
        } catch (final AwsServiceException e) {
            if (Objects.equals(e.awsErrorDetails().errorCode(), "InvalidAccessKeyId")) {
                throw new InvalidSettingsException("Please check your Access Key ID / Secret Key.");
            } else if (Objects.equals(e.awsErrorDetails().errorCode(), "AccessDenied")) {
                setWarningMessage("The credentials provided have restricted permissions. "
                    + "File browsing might not work as expected.\n"
                    + "All buckets will be assumed existing, as they cannot be listed.");
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadInternals(final File nodeInternDir, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {
        setWarningMessage("S3 connection no longer available. Please re-execute the node.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveInternals(final File nodeInternDir, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {
        //nothing to save
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        m_settings.saveSettingsTo(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_settings.validateSettings(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_settings.loadSettingsFrom(settings);
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
        if (m_fsConn != null) {
            m_fsConn.closeInBackground();
            m_fsConn = null;
        }
        m_awsConnectionInfo = null;
        m_fsId = null;
    }
}

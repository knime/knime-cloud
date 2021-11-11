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
 */
package org.knime.cloud.aws.filehandling.s3.node;

import java.io.File;
import java.io.IOException;
import java.util.EnumSet;
import java.util.Objects;

import org.knime.cloud.aws.filehandling.s3.fs.S3CompatibleFSDescriptorProvider;
import org.knime.cloud.aws.filehandling.s3.fs.S3FSConnection;
import org.knime.cloud.aws.filehandling.s3.fs.S3FileSystem;
import org.knime.cloud.aws.filehandling.s3.fs.api.S3FSConnectionConfig;
import org.knime.cloud.aws.filehandling.s3.fs.api.S3FSConnectionConfig.SSEMode;
import org.knime.cloud.aws.filehandling.s3.node.S3ConnectorNodeSettings.CustomerKeySource;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.context.ports.PortsConfiguration;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.filehandling.core.connections.FSConnectionRegistry;
import org.knime.filehandling.core.defaultnodesettings.status.NodeModelStatusConsumer;
import org.knime.filehandling.core.defaultnodesettings.status.StatusMessage.MessageType;
import org.knime.filehandling.core.port.FileSystemPortObject;
import org.knime.filehandling.core.port.FileSystemPortObjectSpec;

import software.amazon.awssdk.awscore.exception.AwsServiceException;

/**
 * Custom S3 compatible endpoint connector node.
 *
 * @author Sascha Wolke, KNIME GmbH
 */

final class S3CompatibleConnectorNodeModel extends NodeModel {
    private static final NodeLogger LOG = NodeLogger.getLogger(S3CompatibleConnectorNodeModel.class);

    private S3CompatibleConnectorNodeSettings m_settings;

    private S3FSConnection m_fsConn;

    private String m_fsId;

    private final NodeModelStatusConsumer m_statusConsumer =
        new NodeModelStatusConsumer(EnumSet.of(MessageType.ERROR, MessageType.WARNING));

    /**
     * The NodeModel for the S3Connection node
     *
     * @param portsConfig Ports configuration
     */
    S3CompatibleConnectorNodeModel(final PortsConfiguration portsConfig) {
        super(portsConfig.getInputPorts(), portsConfig.getOutputPorts());
        m_settings = new S3CompatibleConnectorNodeSettings(portsConfig);
    }

    @Override
    protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        m_fsId = FSConnectionRegistry.getInstance().getKey();
        configureFileChooser(inSpecs);
        m_settings.configureInModel(inSpecs, getCredentialsProvider());
        return new PortObjectSpec[]{createSpec()};
    }

    private void configureFileChooser(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        boolean fileChooserUsed = m_settings.isSseEnabled() && m_settings.getSseMode() == SSEMode.CUSTOMER_PROVIDED
            && m_settings.getCustomerKeySource() == CustomerKeySource.FILE;

        if (fileChooserUsed) {
            m_settings.configureFileChoosersInModel(inSpecs, m_statusConsumer);
            m_statusConsumer.setWarningsIfRequired(this::setWarningMessage);
        }
    }

    private FileSystemPortObjectSpec createSpec() {
        return new FileSystemPortObjectSpec(S3CompatibleFSDescriptorProvider.FS_TYPE.getName(), //
            m_fsId, //
            S3FSConnectionConfig.createCustomS3FSLocationSpec(m_settings.getEndpointURL()));
    }

    @Override
    protected PortObject[] execute(final PortObject[] inObjects, final ExecutionContext exec) throws Exception {
        final S3FSConnectionConfig config = m_settings.toFSConnectionConfig(getCredentialsProvider());
        m_fsConn = new S3FSConnection(config);
        FSConnectionRegistry.getInstance().register(m_fsId, m_fsConn);

        if (config.getConnectionInfo().isUseAnonymous()) {
            setWarningMessage(
                "You are using anonymous credentials. File browsing will only work inside public buckets.");
        } else {
            testFileSystemConnection(m_fsConn);
        }
        return new PortObject[]{new FileSystemPortObject(createSpec())};
    }

    @SuppressWarnings("resource")
    private void testFileSystemConnection(final S3FSConnection fsConn) throws InvalidSettingsException {
        final var fileSystem = (S3FileSystem)fsConn.getFileSystem();
        try {
            fileSystem.getClient().listBuckets();
        } catch (final AwsServiceException e) {
            if (Objects.equals(e.awsErrorDetails().errorCode(), "InvalidAccessKeyId")) {
                throw new InvalidSettingsException("Please check your Access Key ID / Secret Key.", e);
            } else if (Objects.equals(e.awsErrorDetails().errorCode(), "AccessDenied")) {
                final var msg =
                    "The credentials provided have restricted permissions. File browsing might not work as expected.";
                LOG.debug(msg, e);
                setWarningMessage(msg);
            } else {
                setWarningMessage("Unable to test S3 connection using list buckets: "
                    + e.awsErrorDetails().errorMessage() + " For details see View > Open KNIME log.");
                LOG.warn("Unable to test S3 connection using list buckets.", e);
            }
        }
    }

    @Override
    protected void loadInternals(final File nodeInternDir, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {
        setWarningMessage("S3 connection no longer available. Please re-execute the node.");
    }

    @Override
    protected void saveInternals(final File nodeInternDir, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {
        //nothing to save
    }

    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        m_settings.saveSettingsForModel(settings);
    }

    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_settings.validateSettings(settings);
    }

    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_settings.loadSettingsForModel(settings);
    }

    @Override
    protected void onDispose() {
        //close the file system also when the workflow is closed
        reset();
    }

    @Override
    protected void reset() {
        if (m_fsConn != null) {
            m_fsConn.closeInBackground();
            m_fsConn = null;
        }
        m_fsId = null;
    }
}

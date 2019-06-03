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
 *   May 28, 2019 (Julian Bunzel): created
 */
package org.knime.cloud.aws.nodes.connector;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

import org.knime.cloud.aws.s3.filehandler.S3RemoteFileHandler;
import org.knime.cloud.aws.util.AWSConnectionInformationSettings;
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
import org.knime.core.node.defaultnodesettings.SettingsModelAuthentication.AuthenticationType;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.util.Pair;

/**
 * Node model for the Amazon Authentication node.
 *
 * @author Julian Bunzel, KNIME GmbH, Berlin, Germany
 */
class AmazonAuthenticationNodeModel extends NodeModel {

    /** Settings containing information about the AWS connection. */
    private final AWSConnectionInformationSettings m_model = createAWSConnectionModel();

    /** Method to create a new settings object containing information about the AWS connection. */
    static final AWSConnectionInformationSettings createAWSConnectionModel() {
        return new AWSConnectionInformationSettings(null);
    }

    /** Returs a map of authentication type and credentials. */
    static HashMap<AuthenticationType, Pair<String, String>> getNameMap() {
        final HashMap<AuthenticationType, Pair<String, String>> nameMap = new HashMap<>();
        nameMap.put(AuthenticationType.USER_PWD, new Pair<String, String>("Access Key ID and Secret Key",
            "Access Key ID and Secret Access Key based authentication"));
        nameMap.put(AuthenticationType.KERBEROS, new Pair<String, String>("Default Credential Provider Chain",
            "Use the Default Credential Provider Chain for authentication"));
        return nameMap;
    }

    /**
     * Constructor for the node model.
     */
    AmazonAuthenticationNodeModel() {
        super(new PortType[]{}, new PortType[]{AmazonConnectionInformationPortObject.TYPE});
    }

    @Override
    protected PortObject[] execute(final PortObject[] inObjects, final ExecutionContext exec) throws Exception {
        return new PortObject[]{new AmazonConnectionInformationPortObject(createSpec())};
    }

    @Override
    protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        return new PortObjectSpec[]{createSpec()};
    }

    @Override
    protected void loadInternals(final File nodeInternDir, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {
        // Nothing to do.
    }

    @Override
    protected void saveInternals(final File nodeInternDir, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {
        // Nothing to do.
    }

    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        m_model.saveSettingsTo(settings);
    }

    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_model.validateSettings(settings);
    }

    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_model.loadValidatedSettings(settings);
    }

    @Override
    protected void reset() {
        // Nothing to do.
    }

    /**
     * Create the spec, throw exception if no config available.
     *
     * @return ConnectionInformationPortObjectSpec
     * @throws InvalidSettingsException ...
     */
    private CloudConnectionInformationPortObjectSpec createSpec() throws InvalidSettingsException {
        m_model.validateValues();
        final CloudConnectionInformation connectionInformation =
            m_model.createConnectionInformation(getCredentialsProvider(), S3RemoteFileHandler.PROTOCOL);
        return new CloudConnectionInformationPortObjectSpec(connectionInformation);
    }

}

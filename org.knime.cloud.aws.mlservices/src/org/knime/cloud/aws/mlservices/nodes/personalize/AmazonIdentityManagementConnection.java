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
 *   Nov 26, 2019 (simon): created
 */
package org.knime.cloud.aws.mlservices.nodes.personalize;

import org.knime.base.filehandling.remote.files.Connection;
import org.knime.cloud.core.util.port.CloudConnectionInformation;
import org.knime.core.node.NodeLogger;
import org.knime.core.util.KnimeEncryption;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.services.identitymanagement.AmazonIdentityManagement;
import com.amazonaws.services.identitymanagement.AmazonIdentityManagementClientBuilder;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.AssumeRoleRequest;
import com.amazonaws.services.securitytoken.model.AssumeRoleResult;

/**
 * Class used to establish a connection to the Amazon Identity Management.
 *
 * @author Simon Schmid, KNIME GmbH, Konstanz, Germany
 */
public final class AmazonIdentityManagementConnection extends Connection implements AutoCloseable {

    /** Logger instance. */
    private static final NodeLogger LOGGER = NodeLogger.getLogger(AmazonIdentityManagementConnection.class);

    /** AWS connection information */
    private final CloudConnectionInformation m_connectionInformation;

    /** The Amazon Identity Management client */
    private AmazonIdentityManagement m_client;

    /**
     * Creates a new instance of {@code AmazonIdentityManagementConnection}.
     *
     * @param connectionInformation The connection information
     */
    public AmazonIdentityManagementConnection(final CloudConnectionInformation connectionInformation) {
        m_connectionInformation = connectionInformation;
    }

    @Override
    public void open() throws Exception {
        if (!isOpen()) {
            LOGGER.info("Create a new AmazonIdentityManagement in Region \"" + m_connectionInformation.getHost()
                + "\" with connection timeout " + m_connectionInformation.getTimeout() + " milliseconds");
            try {
                if (m_connectionInformation.switchRole()) {
                    m_client = getRoleAssumedIdentityManagementClient(m_connectionInformation);
                } else {
                    m_client = getIdentityManagementClient(m_connectionInformation);
                }
            } catch (final Exception ex) {
                close();
                throw ex;
            }
        }

    }

    /**
     * Creates and returns a new instance of the {@link AmazonIdentityManagement} client.
     *
     * @param connectionInformation The connection information
     * @return AmazonIdentityManagement client
     * @throws Exception thrown if client could not be instantiated
     */
    private static final AmazonIdentityManagement
        getIdentityManagementClient(final CloudConnectionInformation connectionInformation) throws Exception {
        final ClientConfiguration clientConfig =
            new ClientConfiguration().withConnectionTimeout(connectionInformation.getTimeout());

        final AmazonIdentityManagementClientBuilder builder = AmazonIdentityManagementClientBuilder.standard()
            .withClientConfiguration(clientConfig).withRegion(connectionInformation.getHost());

        if (!connectionInformation.useKeyChain()) {
            final AWSCredentials credentials = getCredentials(connectionInformation);
            builder.withCredentials(new AWSStaticCredentialsProvider(credentials));
        }

        return builder.build();
    }

    /**
     * Creates and returns a new instance of the {@link AmazonIdentityManagement} client using rule assumption.
     *
     * @param connectionInformation The connection information
     * @return AmazonIdentityManagement client
     * @throws Exception thrown if client could not be instantiated
     */
    private static final AmazonIdentityManagement
        getRoleAssumedIdentityManagementClient(final CloudConnectionInformation connectionInformation) throws Exception {

        final AWSSecurityTokenServiceClientBuilder builder =
            AWSSecurityTokenServiceClientBuilder.standard().withRegion(connectionInformation.getHost());
        if (!connectionInformation.useKeyChain()) {
            final AWSCredentials credentials = getCredentials(connectionInformation);
            builder.withCredentials(new AWSStaticCredentialsProvider(credentials));
        }

        final AWSSecurityTokenService stsClient = builder.build();

        final AssumeRoleRequest assumeRoleRequest = new AssumeRoleRequest().withRoleArn(buildARN(connectionInformation))
            .withDurationSeconds(3600).withRoleSessionName("KNIME_AmazonIdentityManagement_Connection");

        final AssumeRoleResult assumeResult = stsClient.assumeRole(assumeRoleRequest);

        final BasicSessionCredentials tempCredentials =
            new BasicSessionCredentials(assumeResult.getCredentials().getAccessKeyId(),
                assumeResult.getCredentials().getSecretAccessKey(), assumeResult.getCredentials().getSessionToken());

        final ClientConfiguration clientConfig =
            new ClientConfiguration().withConnectionTimeout(connectionInformation.getTimeout());
        return AmazonIdentityManagementClientBuilder.standard().withClientConfiguration(clientConfig)
            .withCredentials(new AWSStaticCredentialsProvider(tempCredentials))
            .withRegion(connectionInformation.getHost()).build();
    }

    /**
     * Builds an Amazon Resource Name (ARN).
     *
     * @param connectionInformation The connection information
     * @return An ARN
     */
    private static final String buildARN(final CloudConnectionInformation connectionInformation) {
        return "arn:aws:iam::" + connectionInformation.getSwitchRoleAccount() + ":role/"
            + connectionInformation.getSwitchRoleName();
    }

    /**
     * Return a AWSCredentials object,
     *
     * @param connectionInformation The connection information
     * @return The AWSCredentions
     * @throws Exception Thrown if credentials could not be decrypted
     */
    private static final AWSCredentials getCredentials(final CloudConnectionInformation connectionInformation)
        throws Exception {
        final String accessKeyId = connectionInformation.getUser();
        final String secretAccessKey = KnimeEncryption.decrypt(connectionInformation.getPassword());
        return new BasicAWSCredentials(accessKeyId, secretAccessKey);
    }

    @Override
    public boolean isOpen() {
        return m_client != null;
    }

    @Override
    public void close() throws Exception {
        if (isOpen()) {
            m_client.shutdown();
            m_client = null;
        }
    }

    /**
     * Returns an {@code AmazonIdentityManagement} client.
     *
     * @return Returns an AmazonIdentityManagement client
     * @throws Exception Thrown if client could not be created
     */
    public final AmazonIdentityManagement getClient() throws Exception {
        if (!isOpen()) {
            open();
        }
        return m_client;
    }
}

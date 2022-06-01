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
 *   May 28, 2019 (julian): created
 */
package org.knime.cloud.aws.mlservices.nodes.translate;

import org.knime.base.filehandling.remote.files.Connection;
import org.knime.cloud.aws.util.AWSCredentialHelper;
import org.knime.cloud.core.util.port.CloudConnectionInformation;
import org.knime.core.node.NodeLogger;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.translate.AmazonTranslate;
import com.amazonaws.services.translate.AmazonTranslateClientBuilder;

/**
 * Class used to establish the connection to AmazonTranslate.
 *
 * @author Julian Bunzel, KNIME GmbH, Berlin, Germany
 */
public class TranslateConnection extends Connection {

    private static final String ROLE_SESSION_NAME = "KNIME_Translate_Connection";

    /** Logger instance. */
    private static final NodeLogger LOGGER = NodeLogger.getLogger(TranslateConnection.class);

    /** AWS connection information */
    private final CloudConnectionInformation m_connectionInformation;

    /** The AmazonTranslate client */
    private AmazonTranslate m_client;

    /**
     * Creates a new instance of {@code TranslateConnection}.
     *
     * @param connectionInformation The connection information
     */
    TranslateConnection(final CloudConnectionInformation connectionInformation) {
        m_connectionInformation = connectionInformation;
    }

    @Override
    public void open() throws Exception {
        if (!isOpen()) {
            LOGGER.info("Create a new AmazonTranslateClient in Region \"" + m_connectionInformation.getHost()
                + "\" with connection timeout " + m_connectionInformation.getTimeout() + " milliseconds");
            try {
                m_client = getTranslateClient(m_connectionInformation);
            } catch (final Exception ex) {
                close();
                throw ex;
            }
        }

    }

    /**
     * Creates and returns a new instance of the {@link AmazonTranslate} client.
     *
     * @param connInfo The connection information
     * @return AmazonComprehend client
     */
    private static final AmazonTranslate getTranslateClient(final CloudConnectionInformation connInfo) {
        final var clientConfig = new ClientConfiguration() //
            .withConnectionTimeout(connInfo.getTimeout());

        return AmazonTranslateClientBuilder.standard() //
            .withClientConfiguration(clientConfig) //
            .withRegion(connInfo.getHost())
            .withCredentials(AWSCredentialHelper.getCredentialProvider(connInfo, ROLE_SESSION_NAME)) //
            .build();
    }

    @Override
    public boolean isOpen() {
        return m_client != null;
    }

    @Override
    public void close() throws Exception {
        m_client.shutdown();
    }

    /**
     * Returns an {@code AmazonTranslate} client-
     *
     * @return Returns an AmazonTranslate client
     * @throws Exception Thrown if client could not be created
     */
    final AmazonTranslate getClient() throws Exception {
        if (!isOpen()) {
            open();
        }
        return m_client;
    }
}

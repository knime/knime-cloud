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
 *   2021-06-02 (modithahewasinghage): created
 */
package org.knime.cloud.aws.filehandling.s3.fs.api;

import java.net.URI;
import java.time.Duration;

import org.knime.cloud.core.util.port.CloudConnectionInformation;
import org.knime.filehandling.core.connections.meta.FSConnectionConfig;
import org.knime.filehandling.core.connections.meta.base.BaseFSConnectionConfig;

import software.amazon.awssdk.services.s3.model.ServerSideEncryption;

/**
 * {@link FSConnectionConfig} implementation for the Amazon S3 file system.
 *
 * @author modithahewasinghage
 */
public class S3FSConnectionConfig extends BaseFSConnectionConfig {

    /**
     * Default connection timeout for the S3 client.
     */
    public static final int DEFAULT_CONNECTION_TIMEOUT_SECONDS = 30;

    /**
     * Default socket timeout for the S3 client.
     */
    public static final int DEFAULT_SOCKET_TIMEOUT_SECONDS = 30;

    private Duration m_socketTimeout;

    private boolean m_normalizePath;

    private boolean m_sseEnabled;

    private SSEMode m_sseMode;

    private boolean m_sseKmsUseAwsManaged;

    private String m_sseKmsKeyId;

    private String m_customerKey;

    private final CloudConnectionInformation m_connectionInfo;

    private boolean m_endpointOverride = false;

    private URI m_endpointUrl;

    private boolean m_pathStyle = false;

    /**
     *
     * @param workingDirectory
     * @param connectionInfo
     */
    public S3FSConnectionConfig(final String workingDirectory, final CloudConnectionInformation connectionInfo) {
        super(workingDirectory, true);
        m_connectionInfo = connectionInfo;
    }

    /**
     * @return the socketTimeout
     */
    public Duration getSocketTimeout() {
        return m_socketTimeout;
    }

    /**
     * @param socketTimeout the socketTimeout to set
     */
    public void setSocketTimeout(final Duration socketTimeout) {
        m_socketTimeout = socketTimeout;
    }

    /**
     * @return the normalizePath
     */
    public boolean isNormalizePath() {
        return m_normalizePath;
    }

    /**
     * @param normalizePath the normalizePath to set
     */
    public void setNormalizePath(final boolean normalizePath) {
        m_normalizePath = normalizePath;
    }

    /**
     * @return the sseEnabled
     */
    public boolean isSseEnabled() {
        return m_sseEnabled;
    }

    /**
     * @param sseEnabled the sseEnabled to set
     */
    public void setSseEnabled(final boolean sseEnabled) {
        m_sseEnabled = sseEnabled;
    }

    /**
     * @return the sseMode
     */
    public SSEMode getSseMode() {
        return m_sseMode;
    }

    /**
     * @param sseMode the sseMode to set
     */
    public void setSseMode(final SSEMode sseMode) {
        m_sseMode = sseMode;
    }

    /**
     * @return the sseKmsUseAwsManaged
     */
    public boolean isSseKmsUseAwsManaged() {
        return m_sseKmsUseAwsManaged;
    }

    /**
     * @param sseKmsUseAwsManaged the sseKmsUseAwsManaged to set
     */
    public void setSseKmsUseAwsManaged(final boolean sseKmsUseAwsManaged) {
        m_sseKmsUseAwsManaged = sseKmsUseAwsManaged;
    }

    /**
     * @return the sseKmsKeyId
     */
    public String getSseKmsKeyId() {
        return m_sseKmsKeyId;
    }

    /**
     * @param sseKmsKeyId the sseKmsKeyId to set
     */
    public void setSseKmsKeyId(final String sseKmsKeyId) {
        m_sseKmsKeyId = sseKmsKeyId;
    }

    /**
     * @return the customerKey
     */
    public String getCustomerKey() {
        return m_customerKey;
    }

    /**
     * @param customerKey the customerKey to set
     */
    public void setCustomerKey(final String customerKey) {
        m_customerKey = customerKey;
    }

    /**
     * @return the connectionInfo
     */
    public CloudConnectionInformation getConnectionInfo() {
        return m_connectionInfo;
    }

    /**
     * @param override {@code true} if custom endpoint should be used
     */
    public void setOverrideEndpoint(final boolean override) {
        m_endpointOverride = override;
    }

    /**
     * @return {@code true} if a custom endpoint should be used
     */
    public boolean overrideEndpoint() {
        return m_endpointOverride;
    }

    /**
     * @param endpoint URL of endpoint to use
     */
    public void setEndpointUrl(final URI endpoint) {
        m_endpointUrl = endpoint;
    }

    /**
     * @return URL of endpoint
     */
    public URI getEndpointUrl() {
        return m_endpointUrl;
    }

    /**
     * @param pathStyle {@code true} if client should always use path style access
     */
    public void setPathStyle(final boolean pathStyle) {
        m_pathStyle = pathStyle;
    }

    /**
     * @return {@code true} if client should always use path style access
     */
    public boolean usePathStyle() {
        return m_pathStyle;
    }

    /**
     * Enum representing different available S3 server-side encryption modes.
     *
     */
    public enum SSEMode {
            /**
             * SSE-S3 mode
             */
            S3("S3-Managed Keys (SSE-S3)", "SSE-S3", ServerSideEncryption.AES256),
            /**
             * SSE-KMS mode
             */
            KMS("Keys in KMS (SSE-KMS)", "SSE-KMS", ServerSideEncryption.AWS_KMS),
            /**
             * SSE-C mode
             */
            CUSTOMER_PROVIDED("Customer-provided encryption keys (SSE-C)", "SSE-C", null);

        private String m_title;

        private String m_key;

        private ServerSideEncryption m_encryption;

        private SSEMode(final String title, final String key, final ServerSideEncryption encryption) {
            m_title = title;
            m_key = key;
            m_encryption = encryption;
        }

        /**
         * @return the key
         */
        public String getKey() {
            return m_key;
        }

        /**
         * @return the encryption
         */
        public ServerSideEncryption getEncryption() {
            return m_encryption;
        }

        @Override
        public String toString() {
            return m_title;
        }

        /**
         * @return The default mode.
         */
        public static SSEMode getDefault() {
            return S3;
        }

        /**
         * @param key The mode key.
         * @return The mode with the given key or the default mode in case no mode with a given key is found.
         */
        public static SSEMode fromKey(final String key) {
            for (SSEMode mode : values()) {
                if (mode.getKey().equals(key)) {
                    return mode;
                }
            }
            return getDefault();
        }
    }

}

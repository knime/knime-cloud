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
 *   May 31, 2022 (Zkriya Rakhimberdiyev): created
 */
package org.knime.cloud.aws.util;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.time.Duration;

import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;

import org.knime.cloud.core.util.port.CloudConnectionInformation;
import org.knime.core.node.NodeLogger;
import org.knime.core.util.KnimeEncryption;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.AssumeRoleRequest;

/**
 * Utility class to convert a {@link CloudConnectionInformation} into a {@link AWSCredentialsProvider}.
 *
 * @author Zkriya Rakhimberdiyev, Redfield SE
 */
public final class AWSCredentialHelper {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(AWSCredentialHelper.class);

    private static final Duration ASSUME_ROLE_DURATION;

    static {
        // This is the default value of one hour
        var assumeRoleDuration = Duration.ofHours(1);

        final var systemProperty = System.getProperty("knime.cloud.aws.assume_role_duration");
        if (systemProperty != null) {
            try {
                assumeRoleDuration = Duration.ofSeconds(Integer.parseInt(systemProperty));
                LOGGER.debug("Applying knime.cloud.aws.assume_role_duration property: " + assumeRoleDuration);
            } catch (NumberFormatException e) {
                LOGGER.error("Exception parsing knime.cloud.aws.assume_role_duration property: " + e.getMessage());
            }
        }
        ASSUME_ROLE_DURATION = assumeRoleDuration;
    }

    private AWSCredentialHelper() {
    }

    /**
     * Converts a {@link CloudConnectionInformation} into a {@link AWSCredentialsProvider} (AWS SDK v1).
     *
     * @param conInfo CloudConnectionInformation to create credentials for
     * @param roleSessionName role switch session name
     * @return {@link AWSCredentials} for the given {@link CloudConnectionInformation}
     */
    public static AWSCredentialsProvider getCredentialProvider(final CloudConnectionInformation conInfo,
        final String roleSessionName) {

        final AWSCredentialsProvider credentialProvider;

        if (conInfo.useKeyChain()) {
            credentialProvider = new DefaultAWSCredentialsProviderChain();
        } else if (conInfo.isUseAnonymous()) {
            credentialProvider = new AWSStaticCredentialsProvider(new AnonymousAWSCredentials());
        } else {
            final var accessKeyId = conInfo.getUser();
            final var secretAccessKey = decryptSecretAccessKey(conInfo);

            if (conInfo.isUseSessionToken()) {
                credentialProvider = new AWSStaticCredentialsProvider(
                    new BasicSessionCredentials(accessKeyId, secretAccessKey, conInfo.getSessionToken()));
            } else {
                credentialProvider =
                    new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKeyId, secretAccessKey));
            }
        }

        if (conInfo.switchRole()) {
            return getRoleSwitchCredentialProvider(conInfo, credentialProvider, roleSessionName);
        } else {
            return credentialProvider;
        }
    }

    private static String decryptSecretAccessKey(final CloudConnectionInformation con) {
        try {
            return KnimeEncryption.decrypt(con.getPassword());
        } catch (InvalidKeyException | BadPaddingException | IllegalBlockSizeException | IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private static AWSCredentialsProvider getRoleSwitchCredentialProvider(final CloudConnectionInformation con,
        final AWSCredentialsProvider credentialProvider, final String roleSessionName) {

        final var assumeRoleRequest = new AssumeRoleRequest() //
            .withRoleArn(buildARN(con)) //
            .withDurationSeconds((int)ASSUME_ROLE_DURATION.toSeconds()) //
            .withRoleSessionName(roleSessionName);

        final var stsClient = AWSSecurityTokenServiceClientBuilder.standard() //
                .withRegion(con.getHost()) //
                .withCredentials(credentialProvider) //
                .build();

        final var assumeRoleResult = stsClient.assumeRole(assumeRoleRequest);

        return new AWSStaticCredentialsProvider(new BasicSessionCredentials( //
            assumeRoleResult.getCredentials().getAccessKeyId(), //
            assumeRoleResult.getCredentials().getSecretAccessKey(), //
            assumeRoleResult.getCredentials().getSessionToken()));
    }

    private static final String ROLE_SWITCH_ARN_TEMPLATE = "arn:aws:iam::%s:role/%s";

    private static String buildARN(final CloudConnectionInformation conInfo) {
        return String.format(ROLE_SWITCH_ARN_TEMPLATE, //
            conInfo.getSwitchRoleAccount(), //
            conInfo.getSwitchRoleName());
    }
}

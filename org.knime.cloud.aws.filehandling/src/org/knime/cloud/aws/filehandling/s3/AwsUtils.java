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
 *   2020-09-30 (Alexander Bondaletov): created
 */
package org.knime.cloud.aws.filehandling.s3;

import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileSystemException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.security.InvalidKeyException;
import java.util.Base64;

import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;

import org.knime.cloud.core.util.port.CloudConnectionInformation;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.util.KnimeEncryption;

import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

/**
 * Utility class to help with AWS SDK
 *
 * @author Alexander Bondaletov
 */
public final class AwsUtils {
    private AwsUtils() {
    }

    private static final int MOVED_PERMANENTLY = 301;
    private static final int UNAUTHORIZED = 401;
    private static final int FORBIDDEN = 403;
    private static final int NOT_FOUND = 404;

    private static final int CUSTOMER_KEY_SIZE = 32;

    /**
     * Builds appropriate {@link AwsCredentialsProvider} object from the provided {@link CloudConnectionInformation}.
     *
     * @param con The connection information object.
     * @return the {@link AwsCredentialsProvider} object.
     */
    @SuppressWarnings("resource")
    public static AwsCredentialsProvider getCredentialProvider(final CloudConnectionInformation con) {
        final AwsCredentialsProvider credentialProvider;
        final AwsCredentialsProvider conCredentialProvider;

        if (con.useKeyChain()) {
            conCredentialProvider = DefaultCredentialsProvider.builder().build(); // always use a new instance
        } else if (con.isUseAnonymous()) {
            conCredentialProvider = AnonymousCredentialsProvider.create();
        } else {
            final String accessKeyId = con.getUser();
            String secretAccessKey;
            try {
                secretAccessKey = KnimeEncryption.decrypt(con.getPassword());
            } catch (InvalidKeyException | BadPaddingException | IllegalBlockSizeException | IOException e) {
                throw new IllegalStateException(e);
            }
            conCredentialProvider =
                StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKeyId, secretAccessKey));
        }

        if (con.switchRole()) {
            credentialProvider = getRoleSwitchCredentialProvider(con, conCredentialProvider);
        } else {
            credentialProvider = conCredentialProvider;
        }
        return credentialProvider;
    }

    @SuppressWarnings("resource")
    private static AwsCredentialsProvider getRoleSwitchCredentialProvider(final CloudConnectionInformation con,
        final AwsCredentialsProvider credentialProvider) {
        final AssumeRoleRequest asumeRole = AssumeRoleRequest.builder()//
            .roleArn(buildARN(con))//
            .durationSeconds(3600)//
            .roleSessionName("KNIME_S3_Connection")//
            .build();

        final StsClient stsClient = StsClient.builder()//
            .region(Region.of(con.getHost()))//
            .credentialsProvider(credentialProvider)//
            .build();

        return StsAssumeRoleCredentialsProvider.builder()//
            .stsClient(stsClient)//
            .refreshRequest(asumeRole)//
            .asyncCredentialUpdateEnabled(true)//
            .build();
    }

    private static String buildARN(final CloudConnectionInformation connectionInformation) {
        return "arn:aws:iam::" + connectionInformation.getSwitchRoleAccount() + ":role/"
            + connectionInformation.getSwitchRoleName();
    }

    /**
     * Converts provided {@link SdkException} into appropriate {@link IOException}.
     *
     * @param ex The {@link SdkException} instance.
     * @param file A path identifying the file or {@code null} if not known.
     * @param other A path identifying the other file or {@code null} if not known.
     * @return The {@link IOException} instance.
     */
    public static IOException toIOE(final SdkException ex, final Path file, final Path other) {
        if (ex instanceof SdkServiceException) {
            String fileString = file != null ? file.toString() : null;
            String otherString = other != null ? other.toString() : null;

            int status = ((SdkServiceException)ex).statusCode();

            switch (status) {
                case UNAUTHORIZED:
                case FORBIDDEN:
                    final AccessDeniedException ade = new AccessDeniedException(fileString, otherString,
                        String.format("Access denied (HTTP code %d)", status));
                    ade.initCause(ex);
                    return ade;
                case NOT_FOUND:
                    final NoSuchFileException nsfe = new NoSuchFileException(fileString, otherString, null);
                    nsfe.initCause(ex);
                    return nsfe;
                case MOVED_PERMANENTLY:
                    final FileSystemException fse = new FileSystemException(fileString, null,
                        "Bucket belongs to a different region than the client");
                    fse.initCause(ex);
                    return fse;
                default:
                    return new IOException(ex.getMessage(), ex);
            }
        } else {
            return new IOException(ex.getMessage(), ex);
        }
    }

    /**
     * Converts provided {@link SdkException} into appropriate {@link IOException}.
     *
     * @param ex The {@link SdkException} instance.
     * @param file A path identifying the file or {@code null} if not known.
     * @return The {@link IOException} instance.
     */
    public static IOException toIOE(final SdkException ex, final Path file) {
        return toIOE(ex, file, null);
    }

    /**
     * Converts base64 representation of the customer encryption key into byte array.
     *
     * @param base64Key The key in base64 encoding.
     * @return The key bytes.
     * @throws InvalidSettingsException If the key is not a valid base64 string or if the key size is different from 32
     *             bytes.
     */
    public static byte[] getCustomerKeyBytes(final String base64Key) throws InvalidSettingsException {
        try {
            byte[] keyBytes = Base64.getDecoder().decode(base64Key);

            if (keyBytes.length != CUSTOMER_KEY_SIZE) {
                throw new InvalidSettingsException(String.format(
                    "Invalid key length. Expected %d bytes, but got %d bytes", CUSTOMER_KEY_SIZE, keyBytes.length));
            }

            return keyBytes;
        } catch (IllegalArgumentException e) {
            throw new InvalidSettingsException("Encryption key is not a valid base64 string", e);
        }
    }
}

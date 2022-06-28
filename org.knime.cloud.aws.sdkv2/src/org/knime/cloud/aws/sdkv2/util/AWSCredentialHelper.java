package org.knime.cloud.aws.sdkv2.util;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.time.Duration;

import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;

import org.knime.cloud.core.util.port.CloudConnectionInformation;
import org.knime.core.node.NodeLogger;
import org.knime.core.util.KnimeEncryption;

import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

/**
 * Utility class to convert CloudConnectionInformation into AwsCredentialsProvider.
 *
 * @author Zkriya Rakhimberdiyev
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
     * Builds appropriate {@link AwsCredentialsProvider} object from the provided {@link CloudConnectionInformation}.
     *
     * @param conInfo The connection information object.
     * @param roleSessionName role switch session name.
     * @return the {@link AwsCredentialsProvider} object.
     */
    public static AwsCredentialsProvider getCredentialProvider(final CloudConnectionInformation conInfo,
        final String roleSessionName) {

        final AwsCredentialsProvider credentialProvider;

        if (conInfo.useKeyChain()) {
            credentialProvider = DefaultCredentialsProvider.builder().build(); // always use a new instance
        } else if (conInfo.isUseAnonymous()) {
            credentialProvider = AnonymousCredentialsProvider.create();
        } else {
            final var accessKeyId = conInfo.getUser();
            final var secretAccessKey = decryptSecretAccessKey(conInfo);

            if (conInfo.isUseSessionToken()) {
                credentialProvider = StaticCredentialsProvider
                    .create(AwsSessionCredentials.create(accessKeyId, secretAccessKey, conInfo.getSessionToken()));
            } else {
                credentialProvider =
                    StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKeyId, secretAccessKey));
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

    @SuppressWarnings("resource")
    private static AwsCredentialsProvider getRoleSwitchCredentialProvider(final CloudConnectionInformation con,
        final AwsCredentialsProvider credentialProvider, final String roleSessionName) {

        final AssumeRoleRequest asumeRole = AssumeRoleRequest.builder()//
            .roleArn(buildARN(con))//
            .durationSeconds((int)ASSUME_ROLE_DURATION.toSeconds())//
            .roleSessionName(roleSessionName)//
            .build();

        final var stsClient = StsClient.builder()//
            .region(Region.of(con.getHost()))//
            .credentialsProvider(credentialProvider)//
            .build();

        return StsAssumeRoleCredentialsProvider.builder()//
            .stsClient(stsClient)//
            .refreshRequest(asumeRole)//
            .asyncCredentialUpdateEnabled(true)//
            .build();
    }

    private static final String ROLE_SWITCH_ARN_TEMPLATE = "arn:aws:iam::%s:role/%s";

    private static String buildARN(final CloudConnectionInformation conInfo) {
        return String.format(ROLE_SWITCH_ARN_TEMPLATE, //
            conInfo.getSwitchRoleAccount(), //
            conInfo.getSwitchRoleName());
    }
}

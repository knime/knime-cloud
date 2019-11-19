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
 *   Nov 19, 2019 (Sascha Wolke, KNIME GmbH): created
 */
package org.knime.google.cloud.storage.filehandler;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.Signature;
import java.security.SignatureException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.knime.core.node.InvalidSettingsException;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.util.SecurityUtils;

/**
 * Google Cloud Storage helper to sign URLs using the V4 signing process.
 *
 * @see <a href="https://cloud.google.com/storage/docs/access-control/signing-urls-manually">Google docs</a>
 * @author Sascha Wolke, KNIME GmbH
 */
public class GoogleCSUrlSignature {

    private static final long SEVEN_DAYS_SECONDS = 7*24*60*60;
    private static final char COMPONENT_SEPARATOR = '\n';
    private static final String GOOG4_RSA_SHA256 = "GOOG4-RSA-SHA256";
    private static final String SCOPE = "/auto/storage/goog4_request";

    /**
     * Generate a signed public URL with expiration time.
     *
     * @param creds credentials to use for signing
     * @param expirationSeconds URL expiration time in seconds from now
     * @param bucketName bucket name
     * @param objectName object name
     * @return signed URL
     * @throws Exception
     */
    protected static String getSigningURL(final GoogleCredential creds, final long expirationSeconds, final String bucketName,
        final String objectName) throws Exception {

        if (creds == null || StringUtils.isBlank(creds.getServiceAccountId()) || creds.getServiceAccountPrivateKey() == null) {
            throw new InvalidSettingsException("API credentials with service account and priuvate key required.");
        }

        if (expirationSeconds > SEVEN_DAYS_SECONDS) {
            throw new InvalidSettingsException("Expiration Time can't be longer than 604800 seconds (7 days).");
        }

        final Date now = new Date();
        final SimpleDateFormat yearMonthDayFormat = new SimpleDateFormat("yyyyMMdd");
        final SimpleDateFormat exactDateFormat = new SimpleDateFormat("yyyyMMdd'T'HHmmss'Z'");
        yearMonthDayFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        exactDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        final String yearMonthDay = yearMonthDayFormat.format(now);
        final String exactDate = exactDateFormat.format(now);

        final String serviceEmail = creds.getServiceAccountId();
        final String credentialScope = URLEncoder.encode(serviceEmail + "/" + yearMonthDay + SCOPE, "UTF-8");
        final PrivateKey privateKey = creds.getServiceAccountPrivateKey();
        final String canonicalUri = "/" + bucketName + "/" + objectName;
        final String canonicalQuery = constructV4CanonicalQueryString(credentialScope, exactDate, expirationSeconds);
        final String canonicalReqHash = constructV4CanonicalRequestHash(canonicalUri, canonicalQuery);
        final String unsigned = constructV4UnsignedPayload(exactDate, yearMonthDay, canonicalReqHash);
        final String urlSignature = signString(privateKey, unsigned);

        return new StringBuilder()
            .append("https://storage.googleapis.com")
            .append(canonicalUri)
            .append("?")
            .append(canonicalQuery)
            .append("&x-goog-signature=").append(urlSignature)
            .toString();
    }

    private static String constructV4UnsignedPayload(final String exactDate, final String yearMonthDay,
        final String canonicalReqHash) {
        StringBuilder payload = new StringBuilder();
        payload.append(GOOG4_RSA_SHA256).append(COMPONENT_SEPARATOR);
        payload.append(exactDate).append(COMPONENT_SEPARATOR);
        payload.append(yearMonthDay).append(SCOPE).append(COMPONENT_SEPARATOR);
        payload.append(canonicalReqHash);
        return payload.toString();
    }

    private static String constructV4CanonicalQueryString(final String credentialScope, final String exactDate,
        final long expiration) {
        StringBuilder queryString = new StringBuilder();
        queryString.append("x-goog-algorithm=").append(GOOG4_RSA_SHA256).append("&");
        queryString.append("x-goog-credential=").append(credentialScope).append("&");
        queryString.append("x-goog-date=").append(exactDate).append("&");
        queryString.append("x-goog-expires=").append(expiration).append("&");
        queryString.append("x-goog-signedheaders=host");
        return queryString.toString();
    }

    private static String constructV4CanonicalRequestHash(final String canonicalUri, final String canonicalQuery) {
        StringBuilder canonicalRequest = new StringBuilder();
        canonicalRequest.append("GET").append(COMPONENT_SEPARATOR);
        canonicalRequest.append(canonicalUri).append(COMPONENT_SEPARATOR);
        canonicalRequest.append(canonicalQuery).append(COMPONENT_SEPARATOR);
        canonicalRequest.append("host:storage.googleapis.com").append(COMPONENT_SEPARATOR).append(COMPONENT_SEPARATOR);
        canonicalRequest.append("host").append(COMPONENT_SEPARATOR);
        canonicalRequest.append("UNSIGNED-PAYLOAD");
        return DigestUtils.sha256Hex(canonicalRequest.toString());
    }

    private static String signString(final PrivateKey privateKey, final String stringToSign)
        throws UnsupportedEncodingException, NoSuchAlgorithmException, InvalidKeyException, SignatureException {
        Signature signer = SecurityUtils.getSha256WithRsaSignatureAlgorithm();
        byte[] data = stringToSign.getBytes("UTF-8");
        return Hex.encodeHexString(SecurityUtils.sign(signer, privateKey, data));
    }
}

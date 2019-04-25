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
 *   Feb 7, 2019 (jfalgout): created
 */
package org.knime.cloud.aws.comprehend;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.comprehend.AmazonComprehend;
import com.amazonaws.services.comprehend.AmazonComprehendClientBuilder;

/**
 *
 * @author KNIME AG, Zurich, Switzerland
 */
public class ComprehendUtils {

    /** Settings name for the input column name with text to analyze */
    public static final String CFGKEY_COLUMN_NAME = "TextColumnName";

    /** Settings name for the input column name with text to analyze */
    public static final String CFGKEY_SOURCE_LANG = "SourceLanguage";

    /**
     * The config key for the number of threads for parallel tagging.
     */
    public static final String CFGKEY_NUMBER_OF_THREADS = "Number of threads";

    /**
     * The config key for the name of the word tokenizer.
     */
    public static final String CFGKEY_TOKENIZER = "Word tokenizer";

    /**
     * The configuration key for appending the incoming document.
     */
    public static final String CFG_KEY_REPLACE_DOC = "Replace Document";

    /**
     * The configuration key for the new document column name
     */
    public static final String CFG_KEY_NEW_DOCUMENT_COL = "New Document Column Name";

    /**
     * The configuration key for the column containing the documents to process.
     */
    public static final String CFG_KEY_DOCUMENT_COL = "Document Column";

    /**
     * Given a region name, determine if the region supports the Comprehend service.
     *
     * @param regionName
     * @return true if supported, false otherwise
     */
    public static boolean regionSupported(final String regionName) {

        Region region = RegionUtils.getRegion(regionName);
        List<Region> regions = RegionUtils.getRegionsForService(AmazonComprehend.ENDPOINT_PREFIX);
        return regions.contains(region);
    }

    /**
     *
     * @param cxnInfo
     * @return interface to the Comprehend service
     */
    public static AmazonComprehend getClient(final ConnectionInformation  cxnInfo) {

        // Build credentials and provider using account info from the connection object
        // User -> access key ID, password -> secret key
        BasicAWSCredentials creds = new BasicAWSCredentials(cxnInfo.getUser(), cxnInfo.getPassword());
        AWSCredentialsProvider credProvider = new AWSStaticCredentialsProvider(creds);

        // Create a connection to the Comprehend service in the provided region
        AmazonComprehend comprehendClient =
                AmazonComprehendClientBuilder.standard()
                                             .withCredentials(credProvider)
                                             .withRegion(cxnInfo.getHost())  // Region is in the host field
                                             .build();

        return comprehendClient;
    }

    /** Map the part of speech code to a display name */
    public static Map<String, String> POS_MAP;
    static {
        Map<String, String> posMap = new HashMap<>();
        posMap.put("ADJ", "Adjective");
        posMap.put("ADP", "Adposition");
        posMap.put("ADV", "Adverb");
        posMap.put("AUX", "Auxiliary");
        posMap.put("CCONJ", "Coordinating conjunction");
        posMap.put("DET", "Determiner");
        posMap.put("INTJ", "Interjection");
        posMap.put("NOUN", "Noun");
        posMap.put("NUM", "Numeral");
        posMap.put("O", "Other");
        posMap.put("PART", "Participle");
        posMap.put("PRON", "Pronoun");
        posMap.put("PROPN", "Proper noun");
        posMap.put("PUNCT", "Punctuation");
        posMap.put("SCONJ", "Subordinating conjunction");
        posMap.put("SYM", "Symbol");
        posMap.put("VERB", "Verb");
        POS_MAP = Collections.unmodifiableMap(posMap);
    }

    /** Map displayable language name to language code */
    public static Map<String, String> LANG_MAP;
    static {
        Map<String, String> langMap = new HashMap<>();
        langMap.put("English", "en");
        langMap.put("French", "fr");
        langMap.put("German", "de");
        langMap.put("Italian", "it");
        langMap.put("Portuguese", "pt");
        langMap.put("Spanish", "es");

        LANG_MAP = Collections.unmodifiableMap(langMap);
    }

}

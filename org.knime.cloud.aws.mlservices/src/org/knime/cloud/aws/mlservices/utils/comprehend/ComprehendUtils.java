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
package org.knime.cloud.aws.mlservices.utils.comprehend;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.knime.core.node.defaultnodesettings.SettingsModelString;

/**
 * Utility class for Amazon Comprehend nodes.
 *
 * @author Jim Falgout, KNIME AG, Zurich, Switzerland
 */
public class ComprehendUtils {

    /** Empty constructor */
    private ComprehendUtils() {
        // Nothing to do here...
    }

    /** Settings name for the input column name with text to analyze */
    private static final String CFG_KEY_COLUMN_NAME = "text_column_name";

    /** Settings name for the source language. */
    private static final String CFG_KEY_SOURCE_LANG = "source_language";

    /** Default text column. */
    private static final String DEF_TEXT_COL = "";

    /** Default source language. */
    private static final String DEF_SOURCE_LANG = "English";

    /** Batch size used by Comprehend nodes running in batch mode. */
    public static final int BATCH_SIZE = 25;

    /**
     * Creates and returns a new instance of {@link SettingsModelString} storing the name of the text column
     *
     * @return Returns a new instance of {@link SettingsModelString} storing the name of the text column
     */
    public static final SettingsModelString getTextColumnNameModel() {
        return new SettingsModelString(ComprehendUtils.CFG_KEY_COLUMN_NAME, DEF_TEXT_COL);
    }

    /**
     * Creates and returns a new instance of {@link SettingsModelString} storing the source language
     *
     * @return Returns a new instance of {@link SettingsModelString} storing the source language
     */
    public static final SettingsModelString getSourceLanguageModel() {
        return new SettingsModelString(ComprehendUtils.CFG_KEY_SOURCE_LANG, DEF_SOURCE_LANG);
    }

    /** Map displayable language name to language code */
    public static final Map<String, String> LANG_MAP;
    static {
        final Map<String, String> langMap = new HashMap<>();
        langMap.put(DEF_SOURCE_LANG, "en");
        langMap.put("French", "fr");
        langMap.put("German", "de");
        langMap.put("Italian", "it");
        langMap.put("Portuguese", "pt");
        langMap.put("Spanish", "es");

        LANG_MAP = Collections.unmodifiableMap(langMap);
    }

}

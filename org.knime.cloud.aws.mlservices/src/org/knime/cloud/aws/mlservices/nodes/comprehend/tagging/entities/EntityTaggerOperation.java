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
 *   Apr 24, 2019 (jfalgout): created
 */
package org.knime.cloud.aws.mlservices.nodes.comprehend.tagging.entities;

import org.knime.cloud.aws.mlservices.nodes.comprehend.tagging.ComprehendTaggerOperation;
import org.knime.cloud.core.util.port.CloudConnectionInformation;
import org.knime.core.data.DataTableSpec;
import org.knime.ext.textprocessing.nodes.tagging.DocumentTagger;

import com.amazonaws.services.comprehend.AmazonComprehend;

/**
 * The operation that is executed for each partition of the input data. This operation accesses the input
 * {@code Document} column that is to be analyzed and applies the entity tagger to the document. The AWS Comprehend
 * service is used to detect entities within sentences of the document. The tagger creates a new document that contains
 * the tagged entities.
 *
 * @author Jim Falgout, KNIME AG, Switzerland, Zurich
 */
class EntityTaggerOperation extends ComprehendTaggerOperation {

    /**
     * Creates a new instance of {@code EntityTaggerOperation}
     *
     * @param cxnInfo The connection information
     * @param textColumnName The name of the text column
     * @param sourceLanguage The source language
     * @param tokenizerName The name of the word tokenizer
     * @param newColName The name of the new column. {@code null} if original text column should be replaced
     * @param outputTableSpec The output data table spec
     */
    EntityTaggerOperation(final CloudConnectionInformation cxnInfo, final String textColumnName, final String sourceLanguage,
        final String tokenizerName, final String newColName, final DataTableSpec outputTableSpec) {
        super(cxnInfo, textColumnName, sourceLanguage, tokenizerName, newColName, outputTableSpec);
    }

    @Override
    protected DocumentTagger getTagger(final AmazonComprehend comprehendClient, final String languageCode,
        final String tokenizerName) {
        return new EntityTagger(comprehendClient, languageCode, tokenizerName);
    }

}

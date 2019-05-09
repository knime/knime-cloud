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
 *   May 8, 2019 (jfalgout): created
 */
package org.knime.cloud.aws.nodes.tagging.comprehend.sentiment;

import java.util.Arrays;
import java.util.List;

import org.knime.cloud.aws.comprehend.AmazonSentimentTag;
import org.knime.ext.textprocessing.data.Document;
import org.knime.ext.textprocessing.data.Sentence;
import org.knime.ext.textprocessing.data.Tag;
import org.knime.ext.textprocessing.nodes.tagging.AbstractDocumentTagger;
import org.knime.ext.textprocessing.nodes.tagging.TaggedEntity;

import com.amazonaws.services.comprehend.AmazonComprehend;
import com.amazonaws.services.comprehend.model.DetectSentimentRequest;
import com.amazonaws.services.comprehend.model.DetectSentimentResult;

/**
 *
 * @author jfalgout
 */
public class SentimentTagger extends AbstractDocumentTagger {

    private final AmazonComprehend m_client;
    private final String m_languageCode;

    SentimentTagger(final AmazonComprehend client, final String languageCode, final String tokenizerName) {
        super(false, tokenizerName);
        this.m_client = client;
        this.m_languageCode = languageCode;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected List<Tag> getTags(final String tag) {
        return Arrays.asList(AmazonSentimentTag.stringToTag(tag));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected List<TaggedEntity> tagEntities(final Sentence sentence) {

        String textValue = sentence.getTextWithWsSuffix();

        DetectSentimentRequest request =
                new DetectSentimentRequest()
                    .withText(textValue)
                    .withLanguageCode(m_languageCode);

        DetectSentimentResult result = m_client.detectSentiment(request);

        TaggedEntity taggedEnt = new TaggedEntity(textValue, result.getSentiment());

        return Arrays.asList( new TaggedEntity[] { taggedEnt });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void preprocess(final Document doc) {

    }

}

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
package org.knime.cloud.aws.comprehend.entities.node;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.knime.ext.textprocessing.data.Document;
import org.knime.ext.textprocessing.data.Sentence;
import org.knime.ext.textprocessing.data.Tag;
import org.knime.ext.textprocessing.nodes.tagging.AbstractDocumentTagger;
import org.knime.ext.textprocessing.nodes.tagging.TaggedEntity;

import com.amazonaws.services.comprehend.AmazonComprehend;
import com.amazonaws.services.comprehend.model.DetectEntitiesRequest;
import com.amazonaws.services.comprehend.model.DetectEntitiesResult;

/**
 * A tagger that uses the Amazon Comprehend service to detect entities in each sentence
 * of a <code>Document</code>. For each sentence a call is made to the Comprehend service
 * and the returned entities are wrapped with their entity type as a tag.
 *
 * @author KNIME AG
 */

final class EntityTagger extends AbstractDocumentTagger {

    private final AmazonComprehend m_client;
    private final String m_languageCode;

    EntityTagger(final AmazonComprehend client, final String languageCode, final String tokenizerName) {
        super(false, tokenizerName);
        this.m_client = client;
        this.m_languageCode = languageCode;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected List<Tag> getTags(final String tag) {
        return Arrays.asList( new Tag[] { new Tag(tag, "AWS")});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected List<TaggedEntity> tagEntities(final Sentence sentence) {

        String textValue = sentence.getTextWithWsSuffix();

        DetectEntitiesRequest request =
                new DetectEntitiesRequest()
                    .withText(textValue)
                    .withLanguageCode(m_languageCode);

        DetectEntitiesResult result =
            m_client.detectEntities(request);

        List<TaggedEntity> tags =
            result
                .getEntities()
                .stream()
                .map(entity -> new TaggedEntity(entity.getText(), entity.getType()))
                .collect(Collectors.toList());

        return tags;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void preprocess(final Document doc) {

    }

}

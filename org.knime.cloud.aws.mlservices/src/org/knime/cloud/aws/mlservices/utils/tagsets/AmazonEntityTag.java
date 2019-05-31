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
package org.knime.cloud.aws.mlservices.utils.tagsets;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.knime.ext.textprocessing.data.Tag;
import org.knime.ext.textprocessing.data.TagBuilder;

/**
 * Entity tags used by the Amazon Comprehend service.
 *
 * @author Jim Falgout, KNIME
 */
public class AmazonEntityTag implements TagBuilder {

    private enum AmazonEntityTagSet {
            /** A branded product */
            COMMERCIAL_ITEM,
            /** A full date (for example, 11/25/2017), day (Tuesday), month (May), or time (8:30 a.m.) */
            DATE,
            /** An event, such as a festival, concert, election, etc. */
            EVENT,
            /** A specific location, such as a country, city, lake, building, etc. */
            LOCATION,
            /** Large organizations, such as a government, company, religion, sports team, etc. */
            ORGANIZATION,
            /** Entities that don't fit into any of the other entity categories */
            OTHER,
            /** Individuals, groups of people, nicknames, fictional characters */
            PERSON,
            /** A quantified amount, such as currency, percentages, numbers, bytes, etc. */
            QUANTITY,
            /** An official name given to any creation or creative work, such as movies, books, songs, etc. */
            TITLE;
    }

    /** The constant for Entity tag types. */
    private static final String TAG_TYPE = "ENTITY";

    @Override
    public List<String> asStringList() {
        return Stream.of(AmazonEntityTagSet.values()).map(Enum::name).collect(Collectors.toList());
    }

    /**
     * Returns the {@link org.knime.ext.textprocessing.data.Tag} related to the given string. If no corresponding
     * {@link org.knime.ext.textprocessing.data.Tag} is available the {@code OTHER} tag is returned
     *
     * @param str The string representing a {@link org.knime.ext.textprocessing.data.Tag}
     * @return The related {@link org.knime.ext.textprocessing.data.Tag} to the given string
     */
    public static Tag stringToTag(final String str) {
        for (final AmazonEntityTagSet pos : AmazonEntityTagSet.values()) {
            if (pos.name().equals(str)) {
                return new Tag(pos.name(), TAG_TYPE);
            }
        }
        return new Tag(AmazonEntityTagSet.OTHER.name(), TAG_TYPE);
    }

    @Override
    public Tag buildTag(final String value) {
        return AmazonEntityTag.stringToTag(value);
    }

    @Override
    public String getType() {
        return TAG_TYPE;
    }

    @Override
    public Set<Tag> getTags() {
        return Stream.of(AmazonEntityTagSet.values())//
            .map(e -> new Tag(e.name(), TAG_TYPE))//
            .collect(Collectors.toCollection(LinkedHashSet::new));
    }

}

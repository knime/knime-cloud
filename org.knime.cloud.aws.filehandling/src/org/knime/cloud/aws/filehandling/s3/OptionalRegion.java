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
 */
package org.knime.cloud.aws.filehandling.s3;

import org.apache.commons.lang3.StringUtils;

import software.amazon.awssdk.regions.Region;

/**
 * A {@link Region} container that might be empty.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
final class OptionalRegion {

    private final Region m_region;

    private OptionalRegion(final String region) {
        if (StringUtils.isNotBlank(region)) {
            m_region = Region.of(region);
        } else {
            m_region = null;
        }
    }

    private OptionalRegion(final Region region) {
        m_region = region;
    }

    private OptionalRegion() {
        m_region = null;
    }

    static OptionalRegion of(final String region) {
        return new OptionalRegion(region);
    }

    static OptionalRegion of(final Region region) {
        return new OptionalRegion(region);
    }

    static OptionalRegion empty() {
        return new OptionalRegion();
    }

    boolean isEmpty() {
        return m_region == null;
    }

    Region get() {
        return m_region;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((m_region == null) ? 0 : m_region.hashCode());
        return result;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        OptionalRegion other = (OptionalRegion)obj;
        if (m_region == null) {
            if (other.m_region != null) {
                return false;
            }
        } else if (!m_region.equals(other.m_region)) {
            return false;
        }
        return true;
    }
}

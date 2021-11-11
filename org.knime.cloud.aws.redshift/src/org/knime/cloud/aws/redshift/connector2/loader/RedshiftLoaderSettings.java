/*
 * ------------------------------------------------------------------------
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
 * ------------------------------------------------------------------------
 */
package org.knime.cloud.aws.redshift.connector2.loader;

import static java.util.Objects.requireNonNull;

import java.util.Optional;

import org.knime.base.node.io.csvwriter.FileWriterSettings;

/**
 * Additional settings for {@link RedshiftDBLoader}.
 *
 * @author Tobias Koetter, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings("deprecation")
public class RedshiftLoaderSettings {

    private final RedshiftLoaderFileFormat m_fileFormat;

    private final String m_authorization;

    private final String m_compression;

    private final int m_chunkSize;

    private final long m_fileSize;

    private final Optional<FileWriterSettings> m_fileWriterSettings;

    /**
     * Constructs a {@link RedshiftLoaderSettings} object.
     *
     * @param fileFormat the selected intermediate file format
     * @param authorization authorization string
     * @param fileWriterSettings the optional file writer settings
     * @param compression compression method
     * @param chunkSize within file chunk size
     * @param fileSize file size
     */
    public RedshiftLoaderSettings(final RedshiftLoaderFileFormat fileFormat,
        final String authorization, final FileWriterSettings fileWriterSettings, final String compression,
        final int chunkSize, final long fileSize) {
        m_fileFormat = requireNonNull(fileFormat, "fileFormat");
        m_authorization = authorization;
        m_fileWriterSettings = Optional.ofNullable(fileWriterSettings);
        m_compression = compression;
        m_chunkSize = chunkSize;
        m_fileSize = fileSize;
    }

    /**
     * Gets the selected intermediate file format.
     *
     * @return a {@link RedshiftLoaderFileFormat} constant.
     */
    public RedshiftLoaderFileFormat getFileFormat() {
        return m_fileFormat;
    }

    /**
     * @return the authorization
     */
    public String getAuthorization() {
        return m_authorization;
    }

    /**
     * Gets the optional file writer settings.
     *
     * @return {@linkplain Optional optionally} the {@link FileWriterSettings} object or {@linkplain Optional#empty()
     *         empty}.
     */
    public Optional<FileWriterSettings> getFileWriterSettings() {
        return m_fileWriterSettings;
    }

    /**
     * @return the compression
     */
    public String getCompression() {
        return m_compression;
    }

    /**
     * @return the chunkSize
     */
    public int getChunkSize() {
        return m_chunkSize;
    }

    /**
     * @return the fileSize
     */
    public long getFileSize() {
        return m_fileSize;
    }

}

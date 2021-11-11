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

import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

import org.apache.orc.CompressionKind;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.knime.base.node.io.csvwriter.FileWriterSettings;
import org.knime.cloud.aws.redshift.connector2.loader.writer.redshift.RedshiftCsvWriter;
import org.knime.cloud.aws.redshift.connector2.loader.writer.redshift.RedshiftORCWriter;
import org.knime.cloud.aws.redshift.connector2.loader.writer.redshift.RedshiftParquetWriter;
import org.knime.core.node.util.ButtonGroupEnumInterface;
import org.knime.database.node.io.load.impl.fs.util.DBFileWriter;

/**
 * The intermediate file formats supported by the Snowflake data loader node.
 *
 * @author Tobias Koetter, KNIME GmbH, Konstanz, Germany
 */
/**
 * The supported file formats.
 *
 * @author Tobias
 */
@SuppressWarnings("deprecation")
public enum RedshiftLoaderFileFormat implements ButtonGroupEnumInterface {
        /** CSV file format. */
        CSV("CSV", "Comma-separated values", ".csv"),
        /** Apache ORC file format. */
        ORC("ORC", "Apache ORC", ".orc"),
        /** Apache Parquet file format. */
        PARQUET("Parquet", "Apache Parquet", ".parquet") {
            @Override
            public boolean isDefault() {
                return true;
            }
        };

    /** No compression flag which is available for all file formats. */
    public static final String NONE_COMPRESSION = "NONE";

    /**
     * Gets the {@link RedshiftLoaderFileFormat} constant with the specified name.
     *
     * @param name the name of the constant.
     * @return {@linkplain Optional optionally} the {@link RedshiftLoaderFileFormat} constant with the specified name or
     *         {@linkplain Optional#empty() empty}.
     */
    public static Optional<RedshiftLoaderFileFormat> optionalValueOf(final String name) {
        if (name != null && !name.isBlank()) {
            try {
                return Optional.of(valueOf(name));
            } catch (IllegalArgumentException exception) {
                // Ignored.
            }
        }
        return Optional.empty();
    }

    /**
     * Gets the default stage type.
     *
     * @return the default stage type
     */
    public static RedshiftLoaderFileFormat getDefault() {
        for (RedshiftLoaderFileFormat t : values()) {
            if (t.isDefault()) {
                return t;
            }
        }
        return CSV;
    }

    private final String m_fileExtension;

    private final String m_text;

    private final String m_toolTip;

    /**
     * Constructor.
     *
     * @param text text
     * @param toolTip tool tip
     * @param fileExtension file extension
     */
    RedshiftLoaderFileFormat(final String text, final String toolTip, final String fileExtension) {
        m_text = text;
        m_toolTip = toolTip;
        m_fileExtension = fileExtension;
    }

    /**
     * Gets the file extension of the format.
     *
     * @return a file extension string, e.g. {@code ".txt"}.
     */
    String getFileExtension() {
        return m_fileExtension;
    }

    /**
     * Returns the {@link DBFileWriter} to use.
     *
     * @return the {@link DBFileWriter} to use
     */
    DBFileWriter<RedshiftLoaderNodeSettings, RedshiftLoaderSettings> getWriter() {
        switch (this) {
            case CSV:
                return new RedshiftCsvWriter();
            case ORC:
                return new RedshiftORCWriter();
            case PARQUET:
                return new RedshiftParquetWriter();
            default:
                throw new IllegalStateException("Unsupported file format: " + this.name());
        }
    }

    /**
     * Returns the corresponding format part instructions for the COPY command.
     *
     * @param settings {@link RedshiftLoaderSettings} with the user entered settings
     * @return the format part of the Redshift copy command.
     * @see <a href="https://docs.aws.amazon.com/redshift/latest/dg/r_COPY.html">COPY documentation</a>
     */
    String getFormatPart(final RedshiftLoaderSettings settings) {
        final StringBuilder buf = new StringBuilder();
        buf.append("FORMAT ");
        switch (this) {
            case CSV:
                final FileWriterSettings ws = settings.getFileWriterSettings().orElseThrow();
                buf.append("CSV");
                buf.append("\n QUOTE '" + ws.getQuoteBegin() + "'");
                buf.append("\n DELIMITER '" + ws.getColSeparator() + "'");
                buf.append("\n NULL AS '" + ws.getMissValuePattern() + "'");
                buf.append("\n TIMEFORMAT 'YYYY-MM-DDTHH:MI:SS'");
                if (ws.writeColumnHeader()) {
                    buf.append("\n IGNOREHEADER 1\n");
                }
                buf.append("\n ENCODING ");
                buf.append(ws.getCharacterEncoding().replace("-", ""));

                if (!NONE_COMPRESSION.equals(settings.getCompression())) {
                    buf.append("\nGZIP");
                }
                break;
            case ORC:
                buf.append("ORC");
                break;
            case PARQUET:
                buf.append("PARQUET");
                break;
            default:
                throw new IllegalStateException("Unsupported file format: " + this.name());
        }
        return buf.toString();
    }

    /**
     * @return the list of supported compression formats
     */

    List<String> getCompressionFormats() {
        final List<String> compressionFormats = new LinkedList<>();
        compressionFormats.add(NONE_COMPRESSION);
        switch (this) {
            case CSV:
                compressionFormats.add("GZIP");
                break;
            case ORC:
                compressionFormats.add(CompressionKind.SNAPPY.name());
                break;
            case PARQUET:
                compressionFormats.add(CompressionCodecName.SNAPPY.name());
                compressionFormats.add(CompressionCodecName.GZIP.name());
                break;
            default:
                throw new IllegalStateException("Unsupported file format: " + this.name());
        }
        return compressionFormats;

    }

    /**
     * @return the chunk size tool tip per format
     */
    String getChunkSizeToolTipText() {
        switch (this) {
            case CSV:
                return "Not supported";
            case ORC:
                return "Within file Stripe size (rows)";
            case PARQUET:
                return "Within file Row Group size (MB)";
            default:
                throw new IllegalStateException("Unsupported file format: " + this.name());
        }
    }

    /**
     * @return the file size tool tip per format
     */
    String getFileSizeToolTipText() {
        switch (this) {
            case CSV:
                return "Not supported";
            case ORC:
                return "Split data into files of size (rows)";
            case PARQUET:
                return "Split data into files of size (MB)";
            default:
                throw new IllegalStateException("Unsupported file format: " + this.name());
        }
    }

    /**
     * @return the default chunk size per format
     */
    int getDefaultChunkSize() {
        switch (this) {
            case CSV:
                return 1024;
            case ORC:
                return 1024;
            case PARQUET:
                return 128;
            default:
                throw new IllegalStateException("Unsupported file format: " + this.name());
        }
    }

    /**
     * @return the default file size per format
     */
    long getDefaultFileSize() {
        switch (this) {
            case CSV:
                return 1024;
            case ORC:
                return 1000000;
            case PARQUET:
                return 1024;
            default:
                throw new IllegalStateException("Unsupported file format: " + this.name());
        }
    }

    @Override
    public String getText() {
        return m_text;
    }

    @Override
    public String getActionCommand() {
        return name();
    }

    @Override
    public String getToolTip() {
        return m_toolTip;
    }

    @Override
    public boolean isDefault() {
        return false;
    }

}
